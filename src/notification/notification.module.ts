import { Module } from "@nestjs/common";
import { NotificationController } from "./notification.controller";
import { NotificationService } from "./notification.service";
import { TemplateCompilerService } from "./template-compiler.service";
import { PrismaService } from "src/prisma/prisma.service";
import { NotificationProcessor } from "./notification.processor";
import { ProvideFactory } from "./provider/provider.factory";
import { NotificationGateway } from "./notification.gateway";
import { RealTimeAuthController } from "./realtime-notification.controller";
import { RealtimeAuthService } from "./realtime-notification.service";



@Module({
    controllers: [
        NotificationController,
        RealTimeAuthController
    ],
    providers: [
        NotificationService, 
        TemplateCompilerService, 
        PrismaService,
        NotificationProcessor,
        ProvideFactory,
        NotificationGateway,
        RealtimeAuthService
    ]
})
export class NotificationModule {};