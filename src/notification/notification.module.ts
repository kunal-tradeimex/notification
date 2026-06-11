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
import { DistributedRedisLimiterGuard } from "./guards/rate-limiter.guard";
import { BullModule } from "@nestjs/bullmq";
import { NotificationQueue } from "./constants/notification.constants";


@Module({
    imports: [
        // Connect BullMQ to the redis instance configuration
        BullModule.forRoot({
            connection: {
                host: process.env.REDIS_HOST || 'localhost',
                port: parseInt(process.env.REDIS_PORT || '6379',10),
            },
        }),

        // Register the specific queue for our background workers
        BullModule.registerQueue({
            name: NotificationQueue.NOTIFICATION_DELIEVERY
        }),
    ],
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
        RealtimeAuthService,
        DistributedRedisLimiterGuard
    ]
})
export class NotificationModule {};