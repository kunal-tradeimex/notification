
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

    constructor(private readonly prisma: PrismaService) {
        console.log("Websocket initialized")
    }

    async handleConnection(client: Socket) {
        console.log(`🔌 Inbound connection attempt detected. Socket ID: ${client.id}`);
        try {
            // pull credentials from the initial handshake data to validate the tenant and recipient
            const apiKeyHeader = client.handshake.headers['x-api-key'] as string;
            const recipientId = (
                client.handshake.query.recipientId ||
                client.handshake.query.recipientid ||
                client.handshake.headers['recipientid']
             ) as string;

            if (!apiKeyHeader || !recipientId) {
                console.log('❌ Connection rejected: Missing credentials or recipient identifier.');
                client.disconnect(true);
                return ;
            }

            // Authenticate the connection using the secure hashing arch.
            const hashedKey = crypto.createHash('sha256').update(apiKeyHeader).digest('hex');
            const apiKeyRecord = await this.prisma.apiKey.findUnique({
                where: { keyHash: hashedKey },
                select: { tenantId: true, isActive: true }
            });

            if (!apiKeyRecord || !apiKeyRecord.isActive) {
                client.disconnect(true);
                return ;
            }

            const contact = await this.prisma.contact.findUnique({
                where: {
                    tenantId_externalId: {
                        tenantId: apiKeyRecord.tenantId,
                        externalId: recipientId
                    }
                }
            });


            if (!contact || !contact.isActive) {
                client.disconnect(true);
                return ;
            }

            // create the private secure room
            const privateRoomId = `${apiKeyRecord.tenantId}:${recipientId}`;
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
    @OnEvent(NOTIFICATION_CREATED_EVENT)
    handleLiveNotificationPush(payload: NotificationCreatedEvent) {
        const targetRoom = `${payload.tenantId}:${payload.recipientId}`;

        this.server.to(targetRoom).emit('notification_received',payload.notification);

        console.log(`Live Broadcast pushed to room [${targetRoom}] via web socket`);

    }
}