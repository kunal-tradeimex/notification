
import { 
    WebSocketGateway,
    WebSocketServer,
    OnGatewayConnection,
    OnGatewayDisconnect
 } from '@nestjs/websockets';

import { Server,Socket } from 'socket.io';
import { PrismaService } from 'src/prisma/prisma.service';
import * as crypto from 'crypto';
import { OnEvent } from '@nestjs/event-emitter';
import { NOTIFICATION_CREATED_EVENT, NotificationCreatedEvent } from './events/notification-event';
import { Inject, OnModuleInit } from '@nestjs/common';
import { REDIS_CLIENT, REDIS_SUBSCRIBER } from 'src/redis/redis.module';
import Redis from 'ioredis';
import { CacheKeyFactory, REDIS_CHANNELS, WS_EVENTS } from './constants/notification.constants';
import jwt from 'jsonwebtoken';

// confiuration for the websocket to connect here and config the cors

@WebSocketGateway(3002,{
    cors: { 
        origin: '*',
        methods: ['GET', 'POST'],
        allowedHeaders: ['x-api-key', 'recipientid', 'recipientId'],
        credentials: true
    },
    transports: ['websocket','polling'],
    namespace: '/v1/realtime'
})
export class NotificationGateway implements OnGatewayConnection, OnGatewayDisconnect {
    @WebSocketServer()
    server!: Server;

    constructor(
        private readonly prisma: PrismaService,
        @Inject(REDIS_CLIENT) private readonly redis: Redis,
        @Inject(REDIS_SUBSCRIBER) private readonly redisSubscriber: Redis
    ) {
        console.log("Websocket initialized")
    }


    // Initialize cluster subscription on app start so web socket also get events from the redis
    async onModuleInit() {
        // HORIZONTAL SCALING BACKBONE: REDIS PUB/SUB LISTENER
        // LOCK REASON: A Redis connection cannot run data commands (ZADD, GET, SET) 
        // once it enters SUBSCRIBE mode. We use a dedicated client ('redisSubscriber') 
        // to open a persistent, long-lived TCP stream listening to the cluster channel.
        // This allows any app instance in our cloud to catch and stream live alerts 
        // to active UI rooms, even if the notification was processed by a completely 
        // different server node.
        await this.redisSubscriber.subscribe(REDIS_CHANNELS.PLATFORM_NOTIFICATIONS);
        console.log('Gateway Cluster listening actively on channel [platform:notifications]');

        // intercept inbound events from the redis network core
        this.redisSubscriber.on('message', async (channel: string, message: string) => {
            if (channel === REDIS_CHANNELS.PLATFORM_NOTIFICATIONS) {
                try {
                    const payload = JSON.parse(message);
                    const targetRoom = `${payload.tenantId}:${payload.recipientId}`;

                    // Direct Persistent Socket Stream Delievery
                    // If the user isn't connected to that specific server node, socker.io handles it
                    this.server.to(targetRoom).emit(WS_EVENTS.NOTIFICATION_RECEIVED,payload.notification);
                    console.log(`Node caught network event -> streamed to client cell room: [${targetRoom}]`);

                    // perform the write-thrugh caching concurrently right here
                    const score = new Date(payload.notification.createdAt).getTime();
                    const stringifiedData = JSON.stringify(payload.notification);

                    // define the cahce key for storing all notifications and unread notification
                    const cacheKeyAll = CacheKeyFactory.getAllFeedKey(payload.tenantId,payload.recipientId);
                    const cacheKeyUnread = CacheKeyFactory.getAllFeedKey(payload.tenantId,payload.recipientId);

                    // use regular redis client pipeline
                    const pipeline = this.redis.pipeline();


                    // add the data in the pipe line via zadd to use the data to store in the zset
                    pipeline.zadd(cacheKeyAll,score,stringifiedData);
                    pipeline.zadd(cacheKeyUnread,score,stringifiedData);

                    // remove the data from the cache which is too old only use last 100 notification
                    pipeline.zremrangebyrank(cacheKeyAll,0,-101);
                    pipeline.zremrangebyrank(cacheKeyUnread,0,-101);

                    // define the ttl for the value to expire the value after sometime
                    pipeline.expire(cacheKeyAll,604800);
                    pipeline.expire(cacheKeyUnread,604800);

                    // execute the pipeline
                    const result = await pipeline.exec();

                    console.log('Pipeline result:',result);

                    console.log(
                        'Feed:All',
                        await this.redis.zrange(cacheKeyAll,0,-1,'WITHSCORES')
                    );

                    console.log(
                        'Feed Unread:',
                        await this.redis.zrange(cacheKeyUnread,0,-1,'WITHSCORES')
                    );
                } catch (error) {
                    console.error('Node failed to deserialize cluster event payload:',error);
                }
            }
        })
    }

    async handleConnection(client: Socket) {
        console.log(`🔌 Inbound connection attempt detected. Socket ID: ${client.id}`);
        try {
            // pull credentials from the initial handshake data to validate the tenant and recipient (old code verify connection via api-key send via user browser)
            // const apiKeyHeader = client.handshake.headers['x-api-key'] as string;
            // const recipientId = (
            //     client.handshake.query.recipientId ||
            //     client.handshake.query.recipientid ||
            //     client.handshake.headers['recipientid']
            //  ) as string;

            // if (!apiKeyHeader || !recipientId) {
            //     console.log('❌ Connection rejected: Missing credentials or recipient identifier.');
            //     client.disconnect(true);
            //     return ;
            // }

            // // Authenticate the connection using the secure hashing arch.
            // const hashedKey = crypto.createHash('sha256').update(apiKeyHeader).digest('hex');
            // const apiKeyRecord = await this.prisma.apiKey.findUnique({
            //     where: { keyHash: hashedKey },
            //     select: { tenantId: true, isActive: true }
            // });

            // if (!apiKeyRecord || !apiKeyRecord.isActive) {
            //     client.disconnect(true);
            //     return ;
            // }

            // const contact = await this.prisma.contact.findUnique({
            //     where: {
            //         tenantId_externalId: {
            //             tenantId: apiKeyRecord.tenantId,
            //             externalId: recipientId
            //         }
            //     }
            // });


            // if (!contact || !contact.isActive) {
            //     client.disconnect(true);
            //     return ;
            // }


            // New and Optimized code verify web socket connection with the help of two way handshake server-to-server bw naas and user backend

            // console.log("Client Handshake data is:",client.handshake.headers)
            const token = client.handshake.headers.token as string;

            if (!token) {
                console.log('Connection Dropped: Missing token in query');
                client.disconnect();
                return;
            }

            // verify the token and extract the decoded payload
            const secret = process.env.JWT_SECRET_IN_APP_NOTIY;

            if (!secret) {
                console.log('Secret key is missing');
                return ;
            }

            const decoded = jwt.verify(token,secret) as { tenantId: string, recipientId: string };


            const { tenantId,recipientId } = decoded;

            // create the private secure room with the help of decoded from the Jwt
            const privateRoomId = `${tenantId}:${recipientId}`;
            await client.join(privateRoomId);

            console.log(`Client connected securely to stream room: [${privateRoomId}]`);
        } catch (error) {
            console.log(error)
            client.disconnect(true);
        }
    }

    handleDisconnect(client: any) {
        console.log(`Client disconnected from the real-time stream engine: ${client.id}`);
    }


    // Listener for the event
    // Old code without redis pub/sub work when only one instance of server is used 
    // @OnEvent(NOTIFICATION_CREATED_EVENT)
    // async handleLiveNotificationPush(payload: NotificationCreatedEvent) {
    //     const targetRoom = `${payload.tenantId}:${payload.recipientId}`;

    //     // Deliver the notification instantly via the websocket pipe
    //     this.server.to(targetRoom).emit('notification_received',payload.notification);

    //     try {

    //         // Surgical redis cache write-through
    //         // Define the Dynamic keys one to store the all notitifcation and one which store the unread message for easy filtration
    //         const cacheKeyAll = `feed:read:${payload.tenantId}:${payload.recipientId}`;
    //         const cacheKeyUnread = `feed:all:${payload.tenantId}:${payload.recipientId}`;

    //         // use the redis pipeline for the batch write functionalities together
    //         const pipeline = this.redis.pipeline();

    //         // check created At time already exist
    //         if (!payload.notification?.createdAt) {
    //             console.error('Notification missing createdAt');
    //             return ;
    //         }

    //         const score = new Date(payload.notification?.createdAt).getTime();
    //         const stringifiedData = JSON.stringify(payload.notification);

    //         // ZADD adds the single notification into the sorted set timeline instantly
    //         pipeline.zadd(cacheKeyAll,score,stringifiedData);
    //         pipeline.zadd(cacheKeyUnread,score,stringifiedData);

    //         // Mitigate memory leak: Limit total cached history rows per user to 100 entries max
    //         // Trims out old, cold data past index 100 in logarithmic time complexity
    //         pipeline.zremrangebyrank(cacheKeyAll,0,-101);
    //         pipeline.zremrangebyrank(cacheKeyUnread,0,-101);

    //         // set the Rolling for the 7-days TTL  
    //         pipeline.expire(cacheKeyAll,604800);
    //         pipeline.expire(cacheKeyUnread,604800);

    //         await pipeline.exec();

    //         console.log(`⚡ Cached notification surgically inside Redis Sorted Set: ${payload.notification.id}`);

    //     } catch (error) {
    //         console.error('Cache Ingestion Pipeline failure:',error);
    //     }

    //     console.log(`Live Broadcast pushed to room [${targetRoom}] via web socket`);

    // }
}