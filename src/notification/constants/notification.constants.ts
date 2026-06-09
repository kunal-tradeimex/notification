

/**
 * Global distributed Redis pub/sub channel names
 */

export const REDIS_CHANNELS = {
    PLATFORM_NOTIFICATIONS: 'platform:notifications'
} as const



/**
 * Distributed redis rate limiting configurations
 */
export const RATE_LIMIT_CONFIG = {
    // Define the how many request is allowed per sliding time window frame
    MAX_REQUESTS: 100,
    // Time window frame specified in ms
    WINDOW_DURATION_MS: 60000,
}

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
    },

    /**
     * Generate a unique , isolated tracking key for a tenant's rare limit window
     */
    getTenantRateLimitKey (tenantId: string): string {
        return `rate:tenant:${tenantId}`;
    },

    /**
     * Generate the key for the auth api key 
     */
    getApikey (keyhash: string): string {
        return `auth:key:${keyhash}`;
    }
}