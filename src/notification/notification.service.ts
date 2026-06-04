import { Inject, Injectable, NotFoundException, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { TemplateCompilerService } from "./template-compiler.service";
import * as crypto from 'crypto';
import { ChannelType, EventType, NotificationStatus } from "@prisma/client";
import { INotificationService } from "./interfaces/notification-service.interface";
import { NotificationProcessor } from "./notification.processor";
import { GetFeedQueryDto } from "./dtos/get-feed.dto";
import { NOTIFICATION_CREATED_EVENT, NotificationCreatedEvent } from "./events/notification-event";
import { EventEmitter2 } from '@nestjs/event-emitter';
import { REDIS_CLIENT } from "src/redis/redis.module";
import Redis from "ioredis";
import { CacheKeyFactory, REDIS_CHANNELS } from "./constants/notification.constants";


@Injectable()
export class NotificationService implements INotificationService {

    constructor(
      private readonly prisma: PrismaService,
      private readonly compiler: TemplateCompilerService,
      private readonly processor: NotificationProcessor,
      private readonly eventEmitter: EventEmitter2,

      // That redis client injection is work like both redis client and as well as publisher
      // because client and publisher both are non-blocking operation unlike subscriber which is blocking
      // that why we create a seprate client for the subscriber in redis module 
      @Inject(REDIS_CLIENT) private redis: Redis
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
              ? this.compiler.compile(template.subject,bodyData.data)
              : null;

        const finalBody = this.compiler.compile(template.body,bodyData.data);

        let sendToDestinaton = '';
        
        if (template.channel === ChannelType.EMAIL) sendToDestinaton = contact.email || '';
        if (template.channel === ChannelType.SMS) sendToDestinaton = contact.phone || '';
        if (template.channel === ChannelType.IN_APP) sendToDestinaton = bodyData.recipientId || '';

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

        this.processor.processNotification(notification.id)
            .then(async (processedNotification) => {

                if (template.channel === ChannelType.IN_APP) {
                    const liveStreamPayload: NotificationCreatedEvent = {
                        tenantId: tenantId,
                        recipientId: bodyData.recipientId,
                        notification: {
                            id: notification.id,
                            subject: finalSubject || '',
                            body: finalBody,
                            createdAt: notification.createdAt,
                            isRead: false
                        }
                    }

                    // Broadcast this event inside our server's RAM pool so our Gateway can hear it
                    // this.eventEmitter.emit(NOTIFICATION_CREATED_EVENT,liveStreamPayload); // old code for single server instance

                    // if we want to use mutiple server instance then emitting event in the Server's RAM pool is
                    // not working well because it can be a possiblitiy server websocket connection is on server A
                    // and load balance redirect the request of that service to the server B and if emitted event 
                    // present in server B how we send the real time update to the user browser so use redis pub/sub here

                    await this.redis.publish(
                        REDIS_CHANNELS.PLATFORM_NOTIFICATIONS,
                        JSON.stringify(liveStreamPayload)
                    );

                    console.log(`Broadcast event successfully injected into the global Pub/Sub`);

                }

            })
            .catch((err) => {
                console.error('Failed to trigger background processing chain',err);
            })

        // Return Data
        return {
            success: true,
            notificationId: notification.id,
            status: notification.status,
        };

    }


    async sendInAppNotification (
        tenantId: string,
        params: GetFeedQueryDto
    ) {


        // check recepient id is belong to that tenant or not (multi-tenant safety boundaries checks)
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


        // Determine the target redis key based on filters criteria
        const limit = params.limit ? Number(params.limit) : 10;

        // decide the cachekey according to the unreadOnly query filter
        const cachekey = params.unreadOnly
             ? CacheKeyFactory.getUnreadFeedKey(tenantId,params.recipientId)
             : CacheKeyFactory.getAllFeedKey(tenantId,params.recipientId);


        // end offset for redis zest pagination (0-Indexed inclusive bounds)
        const endOffset = limit-1;

        try {
            // first try the high speed and efficient redis search
            const cacheFeed = await this.redis.zrevrange(cachekey,0,endOffset);

            // check cache hit or miss
            if (cacheFeed && cacheFeed.length > 0) {
                console.log(`Cache HIT [${cachekey}] - Fast streaming cache entries`);
                return {
                    success: true,
                    source: 'cache',
                    count: cacheFeed.length,
                    notifications: cacheFeed.map((item) => JSON.parse(item))
                }
            }
        } catch (cacheError) {
            console.log(`Cache isolation read failure, falling back to db:`,cacheError);
        }


        // Database fallback pathway when cache miss
        const whereClause: any = {
            tenantId: tenantId,
            contact: contact.id,
            status: NotificationStatus.SENT,
            channel: ChannelType.IN_APP
        };


        if (params.unreadOnly) {
            whereClause.isRead = false;
        }
        
        const notifications = await this.prisma.notification.findMany({
            where: whereClause,
             select:{
                id: true,
                subject: true,
                body: true,
                isRead: true,
                createdAt: true
             },
            take: limit,
            orderBy: {
                createdAt: 'desc'
            }
        });

        // cache warmup pipeline to fed the cache with the freq data
        if (notifications.length > 0) {
            try {
                const pipeline = this.redis.pipeline();
                notifications.forEach((notif) => {
                    const score = new Date(notif.createdAt).getTime();
                    pipeline.zadd(cachekey,score,JSON.stringify(notif));
                });
                pipeline.expire(cachekey,604800);
                await pipeline.exec();
                console.log(`Cached feed warmed up with the ${notifications.length} entries`)
            } catch (warmupErr) {
                console.error('Cache Warmup skipped',warmupErr);
            }
        }

        return {
            success: true,
            source: 'database',
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