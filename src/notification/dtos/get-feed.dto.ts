// src/notification/dtos/get-feed.dto.ts
import { 
    IsOptional,
    IsString, 
    IsNotEmpty, 
    IsInt, 
    Min, 
    Max, 
    IsBoolean 
   } from 'class-validator';
import { Transform, Type } from 'class-transformer';

export class GetFeedQueryDto {

  @IsString()
  @IsNotEmpty({ message: 'recipientId query parameter is mandatory' })
  recipientId!: string;

  @IsOptional()
  @Type(() => Number) // Safely coerces incoming string parameter to number
  @IsInt()
  @Min(1)
  @Max(100, { message: 'Max limit allowed per page is 100' }) // Protection boundary
  limit?: number = 10; // Sets standard default fallback safely

  @IsOptional()
  @Transform(({ value }) => value === 'true' || value === true) // Properly evaluates string "true" to a real boolean
  @IsBoolean()
  unreadOnly?: boolean = false;
  
}