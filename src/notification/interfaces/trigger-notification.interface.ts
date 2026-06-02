

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


export interface InAppNotificationParams {
    recipientId: string;
    limit: number;
    unread: boolean | undefined
}

export interface InAppNotificationResponse {
    success: Boolean;
    notifications: any;
}