package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKey {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class);


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("batch.size", 400);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String topic = "topic1";
                String key = "id outer " + i + " inner " + j;
                String value = "Hello World from outer " + i + " inner " + j;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info(recordMetadata.toString());
                        log.info("Received a record with key {} and partition {} with offset {}",
                                key, recordMetadata.partition(), recordMetadata.offset());
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
