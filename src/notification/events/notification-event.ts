

// Internal notification channel name
export const NOTIFICATION_CREATED_EVENT = 'notification.created';



export class NotificationCreatedEvent {
    tenantId?: string;
    recipientId?: string;
    notification?: {
        id: string;
        subject: string;
        body: string; 
        createdAt: Date;
        isRead: boolean;
    }
}