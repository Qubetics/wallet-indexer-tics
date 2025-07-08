import { Kafka } from 'kafkajs';
import * as Interface from '../interfaces/kafka.interface';
import { config } from '.';

class KafkaHelper {
    public kafka: any;

    private producer: any;

    constructor() {
        // this.connect();
    }

    async connect() {
        const KAFKA_BROKER = config.config.KAFKA_BROKER.split(',');
        console.log('KAFKA_BROKER', KAFKA_BROKER);
        const kafkaCrednetials: Interface.KafkaCrednetials = {
            clientId: String(config.config.KAFKA_CLIENT_ID), // 'test'
            brokers: KAFKA_BROKER, // ['localhost:9092']
        };
        this.kafka = new Kafka(kafkaCrednetials);
        this.producer = await this.kafka.producer();
        await this.producer.connect();
        console.log('producer connetced');
    }

    public async produceMessage(topic: string, messageData: any) {
        try {
            await this.producer.send({
                topic: topic,
                acks: 1,
                messages: [{ value: JSON.stringify(messageData) }],
            });
        } catch (error) {
            console.log('🔥 ~ ~ produceMessage error', error);
        }
    }

}

export default new KafkaHelper();
