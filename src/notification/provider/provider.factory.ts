import { Injectable } from "@nestjs/common";
import { ChannelType } from "@prisma/client";
import { IProvider } from "./provider.interface";
import { MockEmailProvider } from "./mock-email.provider";
import { MockSmsProvider } from "./mock-sms.provider";



@Injectable()
export class ProvideFactory {

    getProvider(channel: ChannelType, providerName: string): IProvider {

        switch (channel) {

            case ChannelType.EMAIL:
                return new MockEmailProvider();

            case ChannelType.SMS:
                return new MockSmsProvider();

            default:
                throw new Error(`Channel type [${channel}] is currently unsupported by our dispatch factory`)

        }

    }

}