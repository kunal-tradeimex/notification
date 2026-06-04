import { GetFeedQueryDto } from "../dtos/get-feed.dto";
import { 
    TriggerNotificationPayload,
    TriggerNotificationResponse,
    InAppNotificationResponse
 } from "./trigger-notification.interface";

 
 export interface INotificationService {

    triggerNotification(
        apiKeyHeader: string,
        payload: TriggerNotificationPayload
    ) : Promise<TriggerNotificationResponse>


    sendInAppNotification(
        apiKeyHeader: string,
        payoad: GetFeedQueryDto
    ) : Promise<InAppNotificationResponse>
    
 }