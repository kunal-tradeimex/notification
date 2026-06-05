import { Controller,Post, UseGuards, Body } from "@nestjs/common";
import { ApiKeyGuard } from "src/auth/api-key.guard";
import { CurrentTenantId } from "src/auth/decorator/current-tenant.decorator";
import { RealtimeAuthService } from "./realtime-notification.service";









@Controller('v1/realtime')
@UseGuards(ApiKeyGuard)
export class RealTimeAuthController {

    constructor(private readonly authService: RealtimeAuthService) {}

    
    @Post('auth')
    async generateHandshakeToken (
        @CurrentTenantId() tenantId: string,
        @Body() body: {userId: string}
    ) {

        return this.authService.generateHandshakeToken(tenantId,body.userId);

    }

}