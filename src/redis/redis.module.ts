import { Global, Inject, Module, OnApplicationShutdown } from "@nestjs/common";
import Redis from 'ioredis';


export const REDIS_CLIENT = 'REDIS_CLIENT';

@Global()
@Module({
    providers: [
        {
            provide: REDIS_CLIENT,
            useFactory: () => {

                const client = new Redis({
                    host: process.env.REDIS_HOST || 'localhost',
                    port: parseInt(process.env.REDIS_PORT || '6379',10),
                    maxRetriesPerRequest: 3,
                });

                client.on('connect',() => console.log('Redis Connected Successfully'));
                client.on('error', (err) => console.error('Redis Cluster Error:',err));

                return client;
            },
        },
    ],
    exports: [REDIS_CLIENT]
})
export class RedisModule implements OnApplicationShutdown {
    constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {} 

    async onApplicationShutdown() {
        await this.redis.disconnect();
    }
}