/*
  Warnings:

  - Made the column `externalId` on table `contacts` required. This step will fail if there are existing NULL values in that column.
  - Made the column `contactId` on table `notifications` required. This step will fail if there are existing NULL values in that column.
  - Made the column `templateId` on table `notifications` required. This step will fail if there are existing NULL values in that column.

*/
-- AlterEnum
-- This migration adds more than one value to an enum.
-- With PostgreSQL versions 11 and earlier, this is not possible
-- in a single migration. This can be worked around by creating
-- multiple migrations, each migration adding only one value to
-- the enum.


ALTER TYPE "EventType" ADD VALUE 'READ';
ALTER TYPE "EventType" ADD VALUE 'SEEN';

-- DropForeignKey
ALTER TABLE "notifications" DROP CONSTRAINT "notifications_contactId_fkey";

-- DropForeignKey
ALTER TABLE "notifications" DROP CONSTRAINT "notifications_templateId_fkey";

-- AlterTable
ALTER TABLE "contacts" ALTER COLUMN "externalId" SET NOT NULL;

-- AlterTable
ALTER TABLE "notifications" ADD COLUMN     "isRead" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "isSeen" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "readAt" TIMESTAMP(3),
ADD COLUMN     "seenAt" TIMESTAMP(3),
ALTER COLUMN "contactId" SET NOT NULL,
ALTER COLUMN "templateId" SET NOT NULL;

-- CreateIndex
CREATE INDEX "notifications_contactId_isRead_idx" ON "notifications"("contactId", "isRead");

-- AddForeignKey
ALTER TABLE "notifications" ADD CONSTRAINT "notifications_contactId_fkey" FOREIGN KEY ("contactId") REFERENCES "contacts"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "notifications" ADD CONSTRAINT "notifications_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "templates"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
