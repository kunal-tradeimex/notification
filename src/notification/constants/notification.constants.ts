

/**
 * Global distributed Redis pub/sub channel names
 */

export const REDIS_CHANNELS = {
    PLATFORM_NOTIFICATIONS: 'platform:notifications'
} as const



/**
 * Real-time Websocket event payloads emitted to client interfaces
 */

export const WS_EVENTS = {
    NOTIFICATION_RECEIVED: 'notification_received'
}


/**
 * Centralized multi-tenant Redis cache key factory
 * Here enforces the strict template literal structures across the entire application
 */
export const CacheKeyFactory = {
    /**
     * Function to generate the cache key tracking all notification (read, unread and all)
     */
    getAllFeedKey (tenantId: string, recipientId: string): string {
        return `feed:all:${tenantId}:${recipientId}`;
    },

    /**
     * Generate the cache key for tracking stricly unread notification
     */
    getUnreadFeedKey (tenantId: string,recipientId: string) {
        return `feed:unread:${tenantId}:${recipientId}`;
    }
}