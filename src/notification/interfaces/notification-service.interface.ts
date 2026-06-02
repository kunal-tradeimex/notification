import { 
    TriggerNotificationPayload,
    TriggerNotificationResponse
 } from "./trigger-notification.interface";

 
 export interface INotificationService {

    triggerNotification(
        apiKeyHeader: string,
        payload: TriggerNotificationPayload
    ) : Promise<TriggerNotificationResponse>
    
 }