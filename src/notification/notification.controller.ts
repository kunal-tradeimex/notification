import { Body, 
         Controller, 
         HttpCode, 
         HttpStatus, 
         Post,
         Headers, 
         Get,
         Query,
         UseGuards} from "@nestjs/common";
import { NotificationService } from "./notification.service";
import { GetFeedQueryDto } from "./dtos/get-feed.dto";
import { CurrentTenantId } from "src/auth/decorator/current-tenant.decorator";
import { ApiKeyGuard } from "src/auth/api-key.guard";



@Controller('/notifications')
@UseGuards(ApiKeyGuard)
export class NotificationController {

    constructor(private readonly notificationService: NotificationService) {}

    
    @Post('/trigger')
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

        return this.notificationService.sendInMapNotification(tenantId,query);

    }

}