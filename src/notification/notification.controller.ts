import { Body, 
         Controller, 
         HttpCode, 
         HttpStatus, 
         Post,
         Headers } from "@nestjs/common";
import { NotificationService } from "./notification.service";



@Controller('/notifications')
export class NotificationController {

    constructor(private readonly notificationService: NotificationService) {}

    
    @Post('trigger')
    @HttpCode(HttpStatus.CREATED)
    async trigger(
        @Headers('x-api-key') apiKey: string,
        @Body() body: { workflow: string, recipientId: string, data: Record<string,any> }
    ) {

        return this.notificationService.triggerNotification(apiKey,body);

    }

}