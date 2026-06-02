

export interface ProviderSendPayload {
    to: string;
    subject?: string;
    body: string;
    credentials: Record<string,any>
}


export interface IProvider {
    send(payload: ProviderSendPayload): Promise<{success: boolean; messageId: string}>
}