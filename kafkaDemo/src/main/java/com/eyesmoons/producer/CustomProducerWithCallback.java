package com.eyesmoons.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 带回调函数的api
 */
public class CustomProducerWithCallback {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092, hadoop2:9092, hadoop3:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)), new Callback() {

                // 回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        System.out.println(e.getMessage());
                    }
                }
            });
        }
        producer.close();
    }
}