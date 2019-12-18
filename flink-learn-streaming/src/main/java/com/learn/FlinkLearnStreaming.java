package com.learn;

import com.alibaba.fastjson.JSONObject;
import com.learn.entity.IdentityEvent;
import com.learn.entity.SslAccessLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

import static com.learn.constant.ConfigConstant.*;

/**
 * @author wuww
 * @version 1.0
 */
@Slf4j
public class FlinkLearnStreaming {

    private static FlinkKafkaProducer011<String> producer011;
    private static FlinkKafkaConsumer011<String> consumer011;
    private static String bootstrapServer;
    private static int windowTimeMinutes;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        initConfig(args);
        initKafka();
        env
                .addSource(consumer011)
                .map((MapFunction<String, IdentityEvent>) value -> {
                    SslAccessLog sslAccessLog = JSONObject.parseObject(value, SslAccessLog.class);
                    IdentityEvent identityEvent = new IdentityEvent();
                    String identity = sslAccessLog.getGN().substring(0, sslAccessLog.getGN().indexOf('|'));
                    identityEvent.setIdentity(identity);
                    identityEvent.setTime(sslAccessLog.getReceiveTime());
                    return identityEvent;
                })
                .returns(IdentityEvent.class)
                // eventTime必需设置
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<IdentityEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(IdentityEvent element) {
                        return element.getTime();
                    }
                })
                .keyBy((KeySelector<IdentityEvent, String>) IdentityEvent::getIdentity)
                .timeWindow(Time.minutes(windowTimeMinutes))
                .reduce((ReduceFunction<IdentityEvent>) (value1, value2) -> {
                    if (value1.getTime() >= value2.getTime()) {
                        return value1;
                    } else {
                        return value2;
                    }
                })
                .map(JSONObject::toJSONString)
                .addSink(producer011);

        env.execute("log-process-flink-job");

    }

    // 初始化配置
    private static void initConfig(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        bootstrapServer = parameters.get(BOOTSTRAP_SERVERS, "localhost:9092");
        windowTimeMinutes = parameters.getInt(WINDOW_TIME_MINUTES, 1);
    }

    // 创建生产者和消费者
    private static void initKafka() {
        // kafka配置
        Properties consumerProp = new Properties();
        Properties producerProp = new Properties();
        consumerProp.setProperty(BOOTSTRAP_SERVERS, bootstrapServer);
        producerProp.setProperty(BOOTSTRAP_SERVERS, bootstrapServer);
        consumerProp.setProperty(GROUP_ID, "ssl_access_log_group");
        consumer011 = new FlinkKafkaConsumer011<>("ssl_access_log", new SimpleStringSchema(), consumerProp);
        consumer011.setStartFromGroupOffsets();

        producer011 = new FlinkKafkaProducer011<>("ssl_access_dst", new SimpleStringSchema(), producerProp);
    }

}
