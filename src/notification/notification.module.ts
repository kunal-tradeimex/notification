import { Module } from "@nestjs/common";
import { NotificationController } from "./notification.controller";
import { NotificationService } from "./notification.service";
import { TemplateCompilerService } from "./template-compiler.service";
import { PrismaService } from "src/prisma/prisma.service";
import { NotificationProcessor } from "./notification.processor";
import { ProvideFactory } from "./provider/provider.factory";
import { NotificationGateway } from "./notification.gateway";



@Module({
    controllers: [
        NotificationController
    ],
    providers: [
        NotificationService, 
        TemplateCompilerService, 
        PrismaService,
        NotificationProcessor,
        ProvideFactory,
        NotificationGateway
    ]
})
export class NotificationModule {};