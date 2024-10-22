import { Kafka, KafkaConfig, ConsumerConfig, EachMessagePayload } from 'npm:kafkajs';
import fs from 'node:fs';
import path from 'node:path';
import { logInfo } from './utils.ts';

// const topic: string = Deno.args[1] || 'qa.mxc.persistence.rpt.tsys.daily.tddf';
const topic: string = 'qa.mxc.persistence.rpt.tsys.daily.tddf';

const config: KafkaConfig = {
  brokers: ['natl-vps-kaf01:9092','natl-vps-kaf02:9092','natl-vps-kaf03:9092'],
  clientId: 'ec-kafka-to-json',
  // connectionTimeout: 600_000,
  // requestTimeout: 600_000,
};

const consumerConfig: ConsumerConfig = {
  groupId: 'ec-kafka-to-json',
  // sessionTimeout: 600_000,
};


logInfo('Topic to pull:', topic);
const kafka = new Kafka(config);

logInfo('Configuring kafka consumer...');
const consumer = kafka.consumer(consumerConfig);
await consumer.connect();

consumer.subscribe({ topic, fromBeginning: true });

const outputFilename: string = path.join('messages', topic + '-' + new Date().toISOString().replace(/:/g, '-'));

logInfo('Preparing the file writer:', outputFilename);

const fileWriter = fs.createWriteStream(outputFilename);

logInfo('Consumer done, waiting for messages...');
consumer.run({
  eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
    logInfo('Receiving...', topic, partition, message.offset, message.key);

    await new Promise((resolve, reject) => fileWriter.write(JSON.stringify(message) + '\n', err => (err ? reject(err) : resolve(1))));
  },
});

