import { IProvider, ProviderSendPayload } from "./provider.interface";



export class MockSmsProvider implements IProvider {
    async send(payload: ProviderSendPayload) {
        console.log('\n==================================================');
        console.log('✉️  [MOCK EMAIL DISPATCH SUCCESS]');
        console.log(`TO:       ${payload.to}`);
        console.log(`SUBJECT:  ${payload.subject}`);
        console.log(`BODY:     ${payload.body}`);
        console.log(`USING KEY: ${payload.credentials.apiKey || 'DEFAULT_MOCK_KEY'}`);
        console.log('==================================================\n');

        return { 
            success: true,
            messageId: `mock_eml_${Math.random().toString(36).substring(2,9)}`
         }
    }
}