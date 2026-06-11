import { Body, 
         Controller, 
         HttpCode, 
         HttpStatus, 
         Post,
         Headers, 
         Get,
         Query,
         UseGuards,
         Patch,
         Param,
         ParseIntPipe,
         UseInterceptors} from "@nestjs/common";
import { NotificationService } from "./notification.service";
import { GetFeedQueryDto } from "./dtos/get-feed.dto";
import { CurrentTenantId } from "src/auth/decorator/current-tenant.decorator";
import { ApiKeyGuard } from "src/auth/api-key.guard";
import { ReadAllNotificationDto } from "./dtos/read-all.dto";
import { DistributedRedisLimiterGuard } from "./guards/rate-limiter.guard";
import { IdempotencyInterceptor } from "src/interceptors/idempotency.interceptor";



@Controller('/notifications')
@UseGuards(ApiKeyGuard)
export class NotificationController {

    constructor(private readonly notificationService: NotificationService) {}

    
    @Post('/trigger')
    @UseGuards(DistributedRedisLimiterGuard)
    @UseInterceptors(IdempotencyInterceptor)
    @HttpCode(HttpStatus.CREATED)
    async trigger(
        @CurrentTenantId() tenantId: string,
        @Body() body: { workflow: string, recipientId: string, data: Record<string,any> }
    ) {

        return this.notificationService.triggerNotification(tenantId,body);

    }


    @Get('/feed')
    async inAppFeed (
        @CurrentTenantId() tenantId: string,
        @Query() query: GetFeedQueryDto 
    ) {

        return this.notificationService.sendInAppNotification(tenantId,query);

    }


    @Patch('/:id/read')
    async markAsRead (
        @CurrentTenantId() tenantId: string,
        @Param('id') notificationId: string
    ) {

        return this.notificationService.markInAppNotificationAsRead(tenantId,notificationId);

    }


    @Post('/read-all')
    async markAllRead(
        @CurrentTenantId() tenantId: string,
        @Body() body: ReadAllNotificationDto
    ) {

        return this.notificationService.markAllInAppNotificationAsRead(tenantId,body.recipientId);

    }

}