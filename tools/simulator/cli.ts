import { ConfigModule, ConfigService } from '@nestjs/config';
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { kafkaConfig } from '../../src/kafka/kafka.config';
import { generateEvents } from './events';
import { parseSimulatorArgs, SIMULATOR_HELP } from './options';
import { publishEvents } from './publisher';

async function main() {
  const options = parseSimulatorArgs(process.argv.slice(2));
  if (options === null) {
    console.log(SIMULATOR_HELP);
    return;
  }
  // Load .env without importing the application or starting any consumer.
  await ConfigModule.forRoot();
  const config = kafkaConfig(new ConfigService(process.env));
  const kafka = new Kafka({
    clientId: `${config.clientId}-simulator`,
    brokers: config.brokers,
    logLevel: logLevel.NOTHING,
    retry: { retries: 5 },
  });
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  const count = await publishEvents(
    producer,
    config.topic,
    generateEvents(options),
    (event) => {
      console.log(
        JSON.stringify({
          message: 'Simulator event published',
          topic: config.topic,
          eventId: event.eventId,
          tenantId: event.tenantId,
          correlationId: event.correlationId,
          eventType: event.eventType,
          service: event.context.service,
        }),
      );
    },
  );
  console.log(
    JSON.stringify({
      message: 'Simulator completed',
      scenario: options.scenario,
      count,
    }),
  );
}

void main().catch(() => {
  console.error(
    JSON.stringify({
      message:
        'Simulator failed; check arguments, Kafka configuration, and topic availability',
      hint: 'Use --help for usage. A failed run may have published earlier batches.',
    }),
  );
  process.exitCode = 1;
});
