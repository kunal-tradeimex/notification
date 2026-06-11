import { Inject, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { ProvideFactory } from "./provider/provider.factory";
import { ChannelType, EventType, NotificationStatus } from "@prisma/client";
import { Processor, WorkerHost } from "@nestjs/bullmq";
import { NotificationQueue, REDIS_CHANNELS } from "./constants/notification.constants";
import { REDIS_CLIENT } from "src/redis/redis.module";
import Redis from "ioredis";
import { Job } from "bullmq";






@Injectable()
@Processor(NotificationQueue.NOTIFICATION_DELIEVERY)
export class NotificationProcessor extends WorkerHost {


    constructor(
        private prisma: PrismaService,
        private provideFactory: ProvideFactory,
        @Inject(REDIS_CLIENT) private readonly redis: Redis
    ) {
        super();
    }


    // Old code without using the job queue method via bullmq process the whole req
    // async processNotification (notificationId: string): Promise<void> {

    //     try {
    //         // first fetch the all details for the notification instance
    //         const notification = await this.prisma.notification.findUnique({
    //             where: {
    //                 id: notificationId
    //             },
    //             include: {
    //                 tenant: true
    //             }
    //         });

           
    //         if (!notification) {
    //             return ;
    //         }

    //         // update status to processing
    //         await this.prisma.notification.update({
    //             where: { id: notificationId },
    //             data: { status: NotificationStatus.PROCESSING }
    //         });


    //         // fetch the tenant's delievery credentials configuration
    //         const channelConfig = await this.prisma.channelConfig.findUnique({
    //             where: {
    //                 tenantId_channel: {
    //                     tenantId: notification.tenantId,
    //                     channel: notification.channel
    //                 }
    //             }
    //         });


    //         const credentials = (channelConfig?.credentials) as Record<string,any> || {};
    //         const providerName = channelConfig?.provider || 'mock';

    //         // resolve the sender instace via our factory contract
    //         const provider = this.provideFactory.getProvider(notification.channel,providerName);
            
            
    //         // Fire an transmission
    //         const result = await provider.send({
    //             to: notification.to,
    //             subject: notification.subject || undefined   ,
    //             body: notification.body,
    //             credentials,
    //         });

            
    //         let updatedNotification: any;


    //         // If successfull, update the audit table
    //         if (result.success) {
    //             updatedNotification = await this.prisma.notification.update({
    //                 where: { id: notificationId },
    //                 data: {
    //                     status: NotificationStatus.SENT,
    //                     sentAt: new Date()
    //                 }
    //             });
    //         }

    //         // create the notification event
    //         await this.prisma.notificationEvent.create({
    //             data: {
    //                 notificationId,
    //                 event: EventType.SENT,
    //                 metadata: { providerMessageId: result.messageId }
    //             }
    //         });

    //         return updatedNotification;


    //     } catch (error: any) {

    //         console.error(`Background processing failed for notificaiton ${notificationId}:`, error);


    //         await this.prisma.notification.update({
    //             where: {id: notificationId },
    //             data: {
    //                 status: NotificationStatus.FAILED,
    //                 errorMessage: error.message,
    //                 failedAt: new Date()
    //             }
    //         });

    //         await this.prisma.notificationEvent.create({
    //             data: {
    //                 notificationId,
    //                 event: EventType.FAILED,
    //                 metadata: { error: error.message }
    //             }
    //         })

    //     }

    // } 


    /**
     * BullMQ execution hook wrapper
     * This handles jobs popped off the redis queue automaticaly
     */
    async process(job: Job<any,any,string>): Promise<any> {

        // fetch the meta data for the job processing
        const { 
            notificationId,
            tenantId,
            recipientId,
            finalSubject,
            finalBody 
        } = job.data;

        console.log('============= 🎯 QUEUE WORKER ACTIVATED =============');
        console.log(`Processing Job ID: ${job.id} for Notification: ${job.data.notificationId}`);


        console.log(`Queue picked the job ${job.id} for notification id ${notificationId}`);
        
        try {
            
            // fetch complete metadata details via notification id
            const notificaiton = await this.prisma.notification.findUnique({
                where: { id: notificationId },
                include: { tenant: true }
            });

            console.log("NOTIFICATION IS:",notificaiton?.createdAt);

            if (!notificaiton) {
                console.warn(`Notification [${notificationId}] was not found in DB. Drop the Job`);
                return ;
            }

            // change the notitification status in the DB to processing
            await this.prisma.notification.update({
                where: { id: notificationId },
                data: { status: NotificationStatus.PROCESSING }
            });

            
            // fetch the config of the tenant provider from its model
            const channelConfig = await this.prisma.channelConfig.findUnique({
                where: {
                    tenantId_channel: {
                        tenantId: notificaiton.tenantId,
                        channel: notificaiton.channel
                    }
                }
            });

            
            // extract credentials
            const credentials = (channelConfig?.credentials) as Record<string,any> || {};
            const providerName = channelConfig?.provider || 'mock';

            
            // find the exact client configuration via the factory pattern 
            const provider = this.provideFactory.getProvider(notificaiton.channel,providerName);

            // send the outward transmission across thrid party network
            const result = await provider.send({
                to: notificaiton.to,
                subject: notificaiton.subject || undefined,
                body: notificaiton.body,
                credentials
            });

            // after successful delievery marked notification status as sent
            if (result.success) {
                await this.prisma.notification.update({
                    where: { id: notificationId },
                    data: {
                        status: NotificationStatus.SENT,
                        sentAt: new Date()
                    }
                });
            }

            
            // create the notification event in the notificationevent model
            await this.prisma.notificationEvent.create({
                data: {
                    notificationId,
                    event: EventType.SENT,
                    metadata: { providerMessageId: result.messageId }
                }
            });


            // Send in app notification to redis pub/sub live stream
            if (notificaiton.channel === ChannelType.IN_APP) {
                const liveStreamPayload = {
                    tenantId,
                    recipientId: recipientId,
                    notification: {
                        id: notificaiton.id,
                        subject: finalSubject || '',
                        body: finalBody,
                        createdAt: notificaiton.createdAt,
                        isRead: false
                    }
                }

                await this.redis.publish(
                    REDIS_CHANNELS.PLATFORM_NOTIFICATIONS,
                    JSON.stringify(liveStreamPayload)
                );

                console.log(`Realtime live-feed update broadcast to redis cluster`)
            } else {
                throw new Error('Something went wrong in notification Delivery');
            }

            return { status: 'completed', notificationId }


        } catch (error: any) {

            console.error(`Background worker Exec. for notification [${notificationId}]:`, error);

            // changed the state as fail inside the notification status
            await this.prisma.notification.update({
                where: { id: notificationId },
                data: {
                status: NotificationStatus.FAILED,
                errorMessage: error.message,
                failedAt: new Date(),
                },
            });

            await this.prisma.notificationEvent.create({
                data: {
                notificationId,
                event: EventType.FAILED,
                metadata: { error: error.message },
                },
            });

            // poulate error so throw here 
            throw error;

        }

    }

}