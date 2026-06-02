

export interface TriggerNotificationPayload {
    workflow: string;
    recipientId: string;
    data: Record<string,any>;
}


export interface TriggerNotificationResponse {
    success: Boolean;
    notificationId: string;
    status: string;
}