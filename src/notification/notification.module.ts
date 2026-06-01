import { Module } from "@nestjs/common";
import { NotificationController } from "./notification.controller";
import { NotificationService } from "./notification.service";
import { TemplateCompilerService } from "./template-compiler.service";
import { PrismaService } from "src/prisma/prisma.service";



@Module({
    controllers: [NotificationController],
    providers: [NotificationService, TemplateCompilerService, PrismaService]
})
export class NotificationModule {};