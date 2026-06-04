import { Injectable } from "@nestjs/common";
import { IProvider } from "./provider.interface";


@Injectable()
export class InAppProvider implements IProvider {
    async send() {
        return {
            success: true,
            messageId: `inapp-${Date.now()}`,
        };
    }
}