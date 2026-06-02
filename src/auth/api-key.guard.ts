
import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import * as crypto from 'crypto';



@Injectable()
export class ApiKeyGuard implements CanActivate {

    constructor (private prisma: PrismaService) {}


    async canActivate(context: ExecutionContext): Promise<boolean>  {

        const request = context.switchToHttp().getRequest();
        const apiKeyHeader = request.headers['x-api-key'];

        if (!apiKeyHeader) {
            throw new UnauthorizedException('API Key is missing');
        }

        // find the hahsed value of the key
        const hashedKey = crypto.createHash('sha256').update(apiKeyHeader).digest('hex');

        const apiKeyRecord = await this.prisma.apiKey.findUnique({
            where: { keyHash: hashedKey },
            select: { tenantId: true, isActive: true } // for performance optimization
        });

        
        if (!apiKeyRecord || !apiKeyRecord.isActive) {
            throw new UnauthorizedException('Invalid or inactive API Key');
        }


        // attach the tenantId id to the request object so that can be used further places
        request['tenantId'] = apiKeyRecord.tenantId;

        return true; // so request further processed by the controller 

    }

}