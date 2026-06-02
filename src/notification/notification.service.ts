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

}