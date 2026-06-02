import { Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { ProvideFactory } from "./provider/provider.factory";
import { EventType, NotificationStatus } from "@prisma/client";






@Injectable()
export class NotificationProcessor {
    constructor(
        private prisma: PrismaService,
        private provideFactory: ProvideFactory
    ) {}


    async processNotification (notificationId: string): Promise<void> {

        try {
            // first fetch the all details for the notification instance
            const notification = await this.prisma.notification.findUnique({
                where: {
                    id: notificationId
                },
                include: {
                    tenant: true
                }
            });

           
            if (!notification) {
                return ;
            }

            // update status to processing
            await this.prisma.notification.update({
                where: { id: notificationId },
                data: { status: NotificationStatus.PROCESSING }
            });


            // fetch the tenant's delievery credentials configuration
            const channelConfig = await this.prisma.channelConfig.findUnique({
                where: {
                    tenantId_channel: {
                        tenantId: notification.tenantId,
                        channel: notification.channel
                    }
                }
            });


            const credentials = (channelConfig?.credentials) as Record<string,any> || {};
            const providerName = channelConfig?.provider || 'mock';

            // resolve the sender instace via our factory contract
            const provider = this.provideFactory.getProvider(notification.channel,providerName);
            
            
            // Fire an transmission
            const result = await provider.send({
                to: notification.to,
                subject: notification.subject || undefined   ,
                body: notification.body,
                credentials,
            });


            // If successfull, update the audit table
            if (result.success) {
                await this.prisma.notification.update({
                    where: { id: notificationId },
                    data: {
                        status: NotificationStatus.SENT,
                        sentAt: new Date()
                    }
                });
            }

            // create the notification event
            await this.prisma.notificationEvent.create({
                data: {
                    notificationId,
                    event: EventType.SENT,
                    metadata: { providerMessageId: result.messageId }
                }
            });


        } catch (error: any) {

            console.error(`Background processing failed for notificaiton ${notificationId}:`, error);


            await this.prisma.notification.update({
                where: {id: notificationId },
                data: {
                    status: NotificationStatus.FAILED,
                    errorMessage: error.message,
                    failedAt: new Date()
                }
            });

            await this.prisma.notificationEvent.create({
                data: {
                    notificationId,
                    event: EventType.FAILED,
                    metadata: { error: error.message }
                }
            })

        }

    } 
}