package com.ha.demo.kafka;

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
        logger.info("I am a kafka producer.");

//        Create producer properties.
        Properties properties = new Properties();

//        Local cluster
//        properties.setProperty("bootstrap.servers", "localhost:9092");

//        Conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"13uqy3GgJawT980fHGLB8j\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxM3VxeTNHZ0phd1Q5ODBmSEdMQjhqIiwib3JnYW5pemF0aW9uSWQiOjY3NzczLCJ1c2VySWQiOjc3OTUwLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI2NTIyMmE5MC04NTcwLTQzM2ItOTA5Yy0xMTMyOGEwMWNlODYifX0.C3NS0kbOWBgvOddL3rD2r5RY2UV72h0ctRS05PoIc2w\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

//        Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        Create the producer.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create a producer record.
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world!");

//        Send data. (asynchronous)
        producer.send(producerRecord);

//        Flush producer. Tell the producer to send all data and block until done (synchronous). No need to call in prod env.
        producer.flush();

//        Flush and close producer. This includes producer.flush()
        producer.close();
    }
}