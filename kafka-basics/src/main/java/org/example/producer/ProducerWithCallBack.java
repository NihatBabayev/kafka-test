package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("batch.size", 400);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("topic1", "Hello World from iteration number " + i);

                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info(recordMetadata.toString());
                        log.info("Received a record with topic {} and partition {} with offset {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    } else {
                        log.error("Error occurred while sending message");
                    }
                });
            }
            Thread.sleep(500);
        }
        producer.close();

    }
}
