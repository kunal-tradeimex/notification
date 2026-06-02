import { Injectable, NotFoundException, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { TemplateCompilerService } from "./template-compiler.service";
import * as crypto from 'crypto';
import { ChannelType, EventType, NotificationStatus } from "@prisma/client";
import { INotificationService } from "./interfaces/notification-service.interface";
import { NotificationProcessor } from "./notification.processor";
import { GetFeedQueryDto } from "./dtos/get-feed.dto";



@Injectable()
export class NotificationService implements INotificationService {

    constructor(
      private prisma: PrismaService,
      private compiler: TemplateCompilerService,
      private processor: NotificationProcessor  
    ) {}


    async triggerNotification(
        tenantId: string,
        bodyData: { workflow: string; recipientId: string; data: Record<string,any> }
    ) {


        // find recipient
        const contact = await this.prisma.contact.findUnique({
            where: {
                tenantId_externalId: {
                    tenantId: tenantId,
                    externalId: bodyData.recipientId
                }
            }
        });

        if (!contact || !contact.isActive) {
            throw new NotFoundException(`Contact with externalId ${bodyData.recipientId} not found`);
        }

        // match the tenant slug to the workflow requested
        const template = await this.prisma.template.findFirst({
            where: {
                tenantId: tenantId,
                slug: bodyData.workflow
            }
        });

        if (!template || !template.isActive) {
            throw new NotFoundException(`Template/Workflow ${bodyData.workflow} not found`);
        }

        // compile template using our detached compiler enginer
        const finalSubject = template.subject
              ? this.compiler.compile(template.body,bodyData.data)
              : null;

        const finalBody = this.compiler.compile(template.body,bodyData.data);

        let sendToDestinaton = '';
        
        if (template.channel === ChannelType.EMAIL) sendToDestinaton = contact.email || '';
        if (template.channel === ChannelType.SMS) sendToDestinaton = contact.phone || '';

        // save record as pending into the db as pending
        const notification = await this.prisma.notification.create({
            data: {
                tenantId: tenantId,
                contactId: contact.id,
                templateId: template.id,
                channel: template.channel,
                status: NotificationStatus.PENDING,
                to: sendToDestinaton,
                subject: finalSubject,
                body: finalBody,
                events: {
                    create: {
                        event: EventType.CREATED,
                        metadata: { message: 'Notification initialized via API-Trigger' }
                    }
                }
            }
        });

        // fire background action async withour using await

        this.processor.processNotification(notification.id).catch((err) => {
            console.error('Failed to trigger background processing chain',err);
        })

        // Return Data
        return {
            success: true,
            notificationId: notification.id,
            status: notification.status,
        };

    }


    async sendInMapNotification (
        tenantId: string,
        params: GetFeedQueryDto
    ) {


        // check recepient id is belong to that tenant or not 
        const contact = await this.prisma.contact.findUnique({
            where: {
                tenantId_externalId: {
                    tenantId: tenantId,
                    externalId: params.recipientId
                }
            }
        });


        if (!contact || !contact.isActive) {
            throw new NotFoundException(`Contact with that id ${params.recipientId} not found`);
        }


        const shouldLookForRead = !params.unreadOnly;

        
        const notifications = await this.prisma.notification.findMany({
            where: { 
                tenantId: tenantId,
                contactId: contact.id,
                status: NotificationStatus.SENT,
                channel: ChannelType.IN_APP,
                isRead: shouldLookForRead
             },
             select:{
                id: true,
                subject: true,
                body: true,
                isRead: true,
                createdAt: true
             },
            take: params.limit ? Number(params.limit) : 10 ,
            orderBy: {
                createdAt: 'desc'
            }
        });


        return {
            success: true,
            count: notifications.length,
            notifications: notifications
        }

    }

    async markInAppNotificationAsRead (
        tenantId: string,
        notificaitonId: string
    ) {

        // first check notificationId is belong to the tenant or not
        const notification = await this.prisma.notification.findFirst({
            where: {
                tenantId: tenantId,
                id: notificaitonId
            },
            select: { id: true, isRead: true }
        });


        if (!notification) {
            throw new UnauthorizedException('Invalid Notification Id');
        }

       
        // marked notification if that is not marked as read
        if (!notification.isRead) {
            // mutate the notification status flag
            await this.prisma.notification.update({
                where: { id: notificaitonId },
                data: { isRead: true }
            });

            // append the historical audit log node into the notification event
            await this.prisma.notificationEvent.create({
                data: {
                    notificationId: notificaitonId,
                    event: EventType.READ,
                    metadata: {
                        userAgent: 'client-api',
                        timestamp: new Date().toISOString()
                    }
                }
            })
        }

        return {
            success: true,
            message: 'Notification Marked as Read Successfully'
        }

    }


    async markAllInAppNotificationAsRead (
        tenantId: string,
        recipientId: string
    ) {

        // map the external recipient profile string to out internal relational Id
        const contact = await this.prisma.contact.findUnique({
            where: {
                tenantId_externalId: {
                    tenantId: tenantId,
                    externalId: recipientId
                }
            },
            select: { id: true, isActive: true }
        });

        
        if (!contact || !contact.isActive) {
            throw new NotFoundException(`Recipient contact profile target configuration not found`);
        }

        
        const unreadNotification = await this.prisma.notification.findMany({
            where: {
                tenantId: tenantId,
                contactId: contact.id,
                channel: ChannelType.IN_APP,
                status: NotificationStatus.SENT,
                isRead: false
            },
            select: { id: true }
        });

        
        const notificationIdsToUpdate = unreadNotification.map((n) => n.id);

        
        if (unreadNotification.length === 0) {
            return {
                success: true,
                message: 'No unread notification found for this recipient',
                updatedCount: 0
            }
        }

        
        // Instantiate an atomic high-speed single flag to true database tranasaction
        await this.prisma.$transaction([
            // Operation 1 : Flip all target records flag to true in bulk
            this.prisma.notification.updateMany({
                where: {
                    id: { in: notificationIdsToUpdate }
                },
                data: {
                    isRead: true
                }
            }),

            // Operation 2 : Batch insert matching historic timelines using createMany
            this.prisma.notificationEvent.createMany({
                data: notificationIdsToUpdate.map((id) => ({
                    notificationId: id,
                    event: EventType.READ,
                    metadata: {
                    bulkExecutionPath: true,
                    timestamp: new Date().toISOString(),
                    },
                })),
            }),
        ]);

        return {
            success: true,
            message: 'All unread notification marked as read successfully',
            updatedCount: notificationIdsToUpdate.length
        }

    }

}