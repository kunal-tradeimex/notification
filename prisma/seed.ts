import { PrismaClient, ChannelType } from '@prisma/client';
import * as crypto from 'crypto';

const prisma = new PrismaClient();

async function main() {
  console.log('🌱 Starting database seeding...');

  // 1. Create a Test Tenant (e.g., your first customer)
  const tenant = await prisma.tenant.create({
    data: {
      name: 'Sample Corp',
      slug: 'sample-corp',
    },
  });
  console.log(`✅ Created Tenant: ${tenant.name} (${tenant.id})`);

  // 2. Create a Fake Hashed API Key for this Tenant
  // In real life, you show "ntf_live_123..." to the user once, but store the SHA256 hash
  const plainApiKey = 'ntf_live_testkey123456789';
  const keyHash = crypto.createHash('sha256').update(plainApiKey).digest('hex');

  await prisma.apiKey.create({
    data: {
      tenantId: tenant.id,
      name: 'Development Key',
      prefix: 'ntf_live_',
      keyHash: keyHash,
      scopes: ['notification:write', 'notification:read'],
    },
  });
  console.log(`✅ Created API Key for Tenant. Plain key to use in headers: ${plainApiKey}`);

  // 3. Create a Test Contact (The recipient of the notification)
  const contact = await prisma.contact.create({
    data: {
      tenantId: tenant.id,
      externalId: 'user_dev_99', // This represents Acme's User ID in their own system
      email: 'kunal@example.com',
      phone: '+1234567890',
    },
  });
  console.log(`✅ Created Test Contact: ${contact.email} (External ID: ${contact.externalId})`);

  // 4. Create an Email Template
  const emailTemplate = await prisma.template.create({
    data: {
      tenantId: tenant.id,
      name: 'Welcome Email',
      slug: 'welcome-email',
      channel: ChannelType.EMAIL,
      subject: 'Welcome to our platform, {{name}}!',
      body: '<h1>Hello {{name}}!</h1><p>We are thrilled to have you here. Your account is active.</p>',
      variables: ['name'],
    },
  });
  console.log(`✅ Created Email Template: ${emailTemplate.slug}`);

  // 5. Create a Channel Config (Credentials for sending)
  await prisma.channelConfig.create({
    data: {
      tenantId: tenant.id,
      channel: ChannelType.EMAIL,
      provider: 'sendgrid',
      credentials: { apiKey: 'SG.fake_mock_key_for_now' }, // Mock data for now
    },
  });
  console.log('✅ Created Mock Channel Config for SendGrid.');
  
  console.log('🏁 Seeding finished successfully!');
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });