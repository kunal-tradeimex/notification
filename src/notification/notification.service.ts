import { Injectable, NotFoundException, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { TemplateCompilerService } from "./template-compiler.service";
import * as crypto from 'crypto';
import { ChannelType, EventType, NotificationStatus } from "@prisma/client";



@Injectable()
export class NotificationService {

    constructor(
      private prisma: PrismaService,
      private compiler: TemplateCompilerService) {}


    async triggerNotification(
        apiKeyHeader: string,
        bodyData: { workflow: string; recipientId: string; data: Record<string,any> }
    ) {


        if(!apiKeyHeader) {
            throw new UnauthorizedException('API Key is Missing');
        }

        // Authentication (Hash the inbound API key and find the tenant)
        const hashedKey = crypto.createHash('sha256').update(apiKeyHeader).digest('hex');
        const apiKeyRecord = await this.prisma.apiKey.findUnique({
            where: { keyHash: hashedKey },
            include: { tenant: true }
        });

        if (!apiKeyRecord || !apiKeyRecord.isActive) {
            throw new UnauthorizedException('Invalid or inactive api key');
        }

        const tenantId = apiKeyRecord.tenantId;

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

        // Return Data
        return {
            success: true,
            notificationId: notification.id,
            status: notification.status,
        };

    }

}