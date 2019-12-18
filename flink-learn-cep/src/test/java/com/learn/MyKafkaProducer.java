package com.learn;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * kafka消息发送测试类
 *
 * @author wuww
 * @version 1.0
 */
public class MyKafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        try {
            for (int j = 0; j < 100; j++) {
                String testLog = String.format("{" +
                                "\"DATE\": \"%s\"," +
                                "\"flink_time\": \"%s\"," +
                                "\"ip\": \"10.2.69.83\"," +
                                "\"result\": \"成功\"," +
                                "\"CN\": \"肖庆11\"," +
                                "\"GN\": \"%s|00\"," +
                                "\"host_nmae\": \"v_host1\"," +
                                "\"receive_time\": 1576553508000," +
                                "\"url\": \"http://10.2.11.30/portalauth/eai/\"" +
                                "}",
                        new Date().getTime(),
                        dateFormat.format(new Date()),
                        "GN" + RandomUtils.nextInt(1, 10));
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "testTopic", testLog);
                System.out.println(record.value());
                producer.send(record);
                Thread.sleep(1000L);
            }
        } catch (Exception e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        }
        producer.close();
    }

}
