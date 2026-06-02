import { Body, 
         Controller, 
         HttpCode, 
         HttpStatus, 
         Post,
         Headers, 
         Get,
         Query} from "@nestjs/common";
import { NotificationService } from "./notification.service";
import { GetFeedQueryDto } from "./dtos/get-feed.dto";



@Controller('/notifications')
export class NotificationController {

    constructor(private readonly notificationService: NotificationService) {}

    
    @Post('/trigger')
    @HttpCode(HttpStatus.CREATED)
    async trigger(
        @Headers('x-api-key') apiKey: string,
        @Body() body: { workflow: string, recipientId: string, data: Record<string,any> }
    ) {

        return this.notificationService.triggerNotification(apiKey,body);

    }

    @Get('/feed')
    async inAppFeed (
        @Headers('x-api-key') apiKey: string,
        @Query() query: GetFeedQueryDto 
    ) {

        return this.notificationService.sendInMapNotification(apiKey,query);

    }

}