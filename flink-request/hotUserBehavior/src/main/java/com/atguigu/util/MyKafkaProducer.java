package com.atguigu.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer {
    public static void main(String[] args) throws IOException {

        writeTokafka("hotitems");
    }

    private static void writeTokafka(String topic) throws IOException {

        //kafka配置
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","hadoop102:9092");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //定义一个kafka producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(properties);

        //用缓冲流读取文本数据
        BufferedReader bufferedReader = new BufferedReader(new FileReader(""));
        String line;
        while ((line = bufferedReader.readLine())!= null ){
            ProducerRecord<String, String> ProducerRecord = new ProducerRecord<>(topic, line);

            kafkaProducer.send(ProducerRecord);
        }
        kafkaProducer.close();
    }
}

