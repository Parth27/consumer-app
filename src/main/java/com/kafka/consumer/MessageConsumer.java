package com.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.serializer.ArrayListDeserializer;

public class MessageConsumer {
    Consumer<Integer, List<String>> consumer;
    private boolean running = true;

    public MessageConsumer() {
        consumer = getConsumer();
        consumer.subscribe(Collections.singletonList(ConsumerConfig.TOPIC));
    }

    public void run() {
        while (running) {
            //
            ConsumerRecords<Integer, List<String>> consumerRecords = consumer.poll(Duration.ofSeconds(10));
            int totalSize = consumerRecords.count()*10;
            System.out.println(totalSize);
            try {
                Thread.sleep(ConsumerConfig.FREQUENCY * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Consumer<Integer, List<String>> getConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ConsumerConfig.BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", ConsumerConfig.CONSUMER_GROUP);
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getName());
        properties.setProperty("value.deserializer", ArrayListDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }
}
