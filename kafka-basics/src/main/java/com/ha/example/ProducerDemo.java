package com.ha.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());

    public static void main(String[] args) {
//        System.out.println("Hello world!");
        logger.info("Hello world!");

//        Create producer properties.
        Properties properties = new Properties();

//        Local cluster
        properties.setProperty("bootstrap.server", "localhost:9092");

//        Set producer producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        Create the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create a producer record.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("third_topic", "hello world!");

//        Send data.
        producer.send(producerRecord);

//        Flush producer.
        producer.flush();

//        Close producer.
        producer.close();
    }
}