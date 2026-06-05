import { Injectable, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import * as crypto from 'crypto';
import * as jwt from 'jsonwebtoken';






@Injectable()
export class RealtimeAuthService {

    constructor(private readonly prisma: PrismaService) {}

    // Validate server-to-server credentials and mints an ephemeral handshake token
    async generateHandshakeToken(tenantId: string, userId: string) {

        // validate first that data with tenantId and userId present or valid and apply tenant boundary
        const contact = await this.prisma.contact.findUnique({
            where: {
                tenantId_externalId: {
                    tenantId: tenantId,
                    externalId: userId
                }
            }
        });

        if (!contact || !contact.isActive) {
            throw new UnauthorizedException('Target Recipient is not registered or is suppressed');
        }


        // Generate cryptographically secured temporary token
        const secret = process.env.JWT_SECRET_IN_APP_NOTIY || "SDFSADFSDNF8HBKbhbhbbibkjjkIVAI";

        const token = jwt.sign(
            {
                tenantId: tenantId,
                recipientId: contact.externalId 
            },
            secret,
            { expiresIn: '1h' }
        );

        return { token }

    }
}