import { CanActivate, ExecutionContext, HttpException, HttpStatus, Inject, Injectable } from "@nestjs/common";
import Redis from "ioredis";
import * as crypto from 'crypto';
import { PrismaService } from "src/prisma/prisma.service";
import { REDIS_CLIENT } from "src/redis/redis.module";
import { RATE_LIMIT_CONFIG,CacheKeyFactory } from '../constants/notification.constants'

@Injectable()
export class DistributedRedisLimiterGuard implements CanActivate {

    constructor(
        @Inject(REDIS_CLIENT) private readonly redis: Redis,
        private readonly prisma: PrismaService
    ) {}


    
    async canActivate(context: ExecutionContext): Promise<boolean> {
        const request = context.switchToHttp().getRequest();
        const rawApiKey = request.headers['x-api-key'];

                console.log("DISTRU MO")


        if (!rawApiKey) {
            return true; // Pass to auth guard
        }

        try {

          console.log('--- 🛡️ GUARD TRIGGERED ---');
          console.log('Raw API Key Present:', !!rawApiKey);
            
          // hashing of the current incoming api key
          const keyHash = crypto.createHash('sha256').update(rawApiKey).digest('hex');
          const apiKeyCache = CacheKeyFactory.getApikey(keyHash);
          
          // first search the tenant id along the redis store along apikey
          let tenantId = await this.redis.get(apiKeyCache);

          console.log('Tenant ID Found:',tenantId);

          // if cache miss then try to find in the database
          if (!tenantId) {
            const apiKeyRecord = await this.prisma.apiKey.findUnique({
                where: { keyHash }
            });

            if (!apiKeyRecord || !apiKeyRecord.isActive) {
                return true;
            }

            tenantId = apiKeyRecord.tenantId;

            // now save the tenant id along the apikeyhash as a key so retrived faster next time
            await this.redis.set(apiKeyCache,tenantId,'EX',86400);
            console.log(`Cache Miss: Loaded tenant [${tenantId} from DB and cached that in the Redis]`);
          }

          // Sliding window rate limiting integration
          const rateLimitKey = CacheKeyFactory.getTenantRateLimitKey(tenantId);
          const now = Date.now();

          const clearBeforeScore = now - RATE_LIMIT_CONFIG.WINDOW_DURATION_MS;

          const pipeline = this.redis.pipeline();

          pipeline.zremrangebyscore(rateLimitKey,'-inf',clearBeforeScore);
          pipeline.zadd(rateLimitKey,now,`${now}:${crypto.randomUUID().substring(0,6)}`);
          pipeline.zcard(rateLimitKey);
          pipeline.expire(rateLimitKey, Math.ceil(RATE_LIMIT_CONFIG.WINDOW_DURATION_MS / 1000));

          const results = await pipeline.exec();

          if (!results) {
            return true;
          }

          const totalRequestInWindow = results[2][1] as number;

          console.log(`📊 Redis ZSET Count for Tenant [${tenantId}]: ${totalRequestInWindow} / ${RATE_LIMIT_CONFIG.MAX_REQUESTS}`);

          // short circuit immed. if that is spam
          if (totalRequestInWindow > RATE_LIMIT_CONFIG.MAX_REQUESTS) {
            throw new HttpException(
                {
                    statusCode: HttpStatus.TOO_MANY_REQUESTS,
                    error: 'Too many Request',
                    message: 'Rate Limit threshold exceeded. Dynamic shield blocked request'
                },
                HttpStatus.TOO_MANY_REQUESTS
            );
          }

          return true;

        } catch (error) {

            if (error instanceof HttpException) throw error;
            console.error('Rate Limiter runtime exception:', error);
            return true;
            
        }
    }

}