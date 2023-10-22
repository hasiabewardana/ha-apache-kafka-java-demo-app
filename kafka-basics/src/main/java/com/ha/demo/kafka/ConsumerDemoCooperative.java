package com.ha.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getName());

    public static void main(String[] args) {
//        System.out.println("Hello world!");
        log.info("I am a kafka consumer with shutdown.");

        String groupId = "my-java-application";
        String topic = "demo_java";

//        Create producer properties.
        Properties properties = new Properties();

//        Local cluster
//        properties.setProperty("bootstrap.servers", "localhost:9092");

//        Conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"13uqy3GgJawT980fHGLB8j\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxM3VxeTNHZ0phd1Q5ODBmSEdMQjhqIiwib3JnYW5pemF0aW9uSWQiOjY3NzczLCJ1c2VySWQiOjc3OTUwLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI2NTIyMmE5MC04NTcwLTQzM2ItOTA5Yy0xMTMyOGEwMWNlODYifX0.C3NS0kbOWBgvOddL3rD2r5RY2UV72h0ctRS05PoIc2w\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

//        Set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
//        properties.setProperty("group.instance.id", ""); // Strategy for static assignment.

//        Create the consumer.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        Get a reference to the main thread.
        final Thread mainThread = Thread.currentThread();

//        Adding a shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown. Calling consumer.wakeup()");
                consumer.wakeup();

//                Join the main thread to allow the execution of the main thread.
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
//        Subscribe to a topic.
            consumer.subscribe(List.of(topic));

//        Poll data. (asynchronous)
            while (true) {
//                log.info("Polling");

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
                    log.info("Partition: " + consumerRecord.partition() + " Offset: " + consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown.");
        } catch (Exception e) {
            log.info("unexpected exception in the consumer: ", e);
        }finally {
            consumer.close();
            log.info("The consumer is now gracefully shutdown.");
        }
    }
}