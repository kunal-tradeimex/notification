import { BadRequestException, CallHandler, ConflictException, ExecutionContext, Inject, Injectable, NestInterceptor } from "@nestjs/common";
import Redis from "ioredis";
import { Observable,of, tap } from "rxjs";
import { CacheKeyFactory } from "src/notification/constants/notification.constants";
import { REDIS_CLIENT } from "src/redis/redis.module";
import * as crypto from 'crypto';


enum RequestStatus{
    PROCESSING = 'PROCESSING',
    RESOLVED = 'RESOLVED'
}


@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {

    constructor (@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

    async intercept(context: ExecutionContext, next: CallHandler): Promise<Observable<any>> {
        
        const request = context.switchToHttp().getRequest();

        // apply idempotency for the post call only
        if (request.method !== 'POST') {
            return next.handle();
        }

        // get idempotency key from req header and tenant Id from req
        const idempotencyKey = request.headers['x-idempotency-key'];
        const tenantId = request['tenantId'];

        // validation on the idempotency key
        if (!idempotencyKey) {
            throw new BadRequestException('Missing header of idempotency key');
        }

        const redisIdempotencyKey = CacheKeyFactory.getIdempotencyKey(tenantId,idempotencyKey);

        
        // generate a fingerprint hash of the request body to prevent key resue with diff. payload
        const currentBodyHash = crypto
            .createHash('sha256')
            .update(JSON.stringify(request.body || {}))
            .digest('hex');

        
        // look on the redis to see key exist
        const cacheRecord = await this.redis.hgetall(redisIdempotencyKey);

        if (Object.keys(cacheRecord).length > 0) {
            // validate that the request body matches the origina exec fingerprint
            if (cacheRecord.request_hash !== currentBodyHash) {
                throw new BadRequestException(
                    'Idempotency key reuse error: Request body payload does not match original one'
                );
            }

            // if orginal request is still bust in the node server or prisma db
            if (cacheRecord.status === RequestStatus.PROCESSING) {
                throw new ConflictException(
                    'Idempotency lock active: A Duplicate request is currently being processed. Please Wait'
                );
            }

            if (cacheRecord.status === RequestStatus.RESOLVED) {
                console.log(`Idempotency Hit: Return cached response for key [${idempotencyKey}]`);
                const cacheResponse = JSON.parse(cacheRecord.response_body);
                return of(cacheResponse);
            }

        }

            // Atomic Locking: Set state to PROCESSING with a 24-hrs 
            await this.redis.hset(redisIdempotencyKey,{
                status: RequestStatus.PROCESSING,
                request_hash: currentBodyHash
            });

            await this.redis.expire(redisIdempotencyKey,86400);


            // now pass the request to the notification service 
            return next.handle().pipe(
                tap(async (responseBody) => {
                    await this.redis.hset(redisIdempotencyKey, {
                        status: RequestStatus.RESOLVED,
                        response_body: JSON.stringify(responseBody),
                    });
                    console.log(`Idempotency Saved: Cache Successfull transactions for key [${idempotencyKey}]`);
                })
            );
    }

}