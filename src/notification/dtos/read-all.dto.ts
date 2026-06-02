import { IsNotEmpty, IsString } from "class-validator";


export class ReadAllNotificationDto {

    @IsString()
    @IsNotEmpty({ message: "recipientId body parameter is required" })
    recipientId!: string

}