import { Global, Inject, Module, OnApplicationShutdown } from "@nestjs/common";
import Redis from 'ioredis';


export const REDIS_CLIENT = 'REDIS_CLIENT';
export const REDIS_SUBSCRIBER = 'REDIS_SUBSCRIBER';

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

                client.on('connect',async () => {
                    console.log('Redis Client Connected Successfully');

                    try {
                        const pong = await client.ping();
                        console.log("PING:",pong);

                        const keys = await client.keys('*');
                        console.log("KEYS:",keys);

                    } catch (error) {
                        console.log(error);
                    }
                });

                client.on('error', (err) => console.error('Redis Cluster Error:',err));

                

                return client;
            },
        },
        {
            provide: REDIS_SUBSCRIBER,
            useFactory: () => {

                const client = new Redis({
                    host: process.env.REDIS_HOST || 'localhost',
                    port: parseInt(process.env.REDIS_PORT || '6379',10),
                    maxRetriesPerRequest: null,
                })

                console.log(client.ping());
                console.log(client.keys('*'))

                client.on('connect',() => console.log('Redis Subscriber Connected Successfully'));;
                client.on('error', (err) => console.error('Redis Subscriber error:',err));

                return client;
            }
        }
    ],
    exports: [REDIS_CLIENT,REDIS_SUBSCRIBER]
})
export class RedisModule implements OnApplicationShutdown {
    constructor(
        @Inject(REDIS_CLIENT) private readonly redis: Redis,
        @Inject(REDIS_SUBSCRIBER) private readonly subscriber: Redis
    ) {} 

    async onApplicationShutdown() {
        await this.redis.disconnect();
        await this.subscriber.disconnect();
    }
}