package com.learn;

import com.alibaba.fastjson.JSONObject;
import com.learn.entity.IdentityEvent;
import com.learn.entity.SslAccessLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author wuww
 * @version 1.0
 */
@Slf4j
public class FlinkLearnCep {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("testTopic", new SimpleStringSchema(), properties);
        consumer011.setStartFromLatest();

        SingleOutputStreamOperator<IdentityEvent> stream = env
                .addSource(consumer011)
                .map((MapFunction<String, IdentityEvent>) value -> {
                    SslAccessLog sslAccessLog = JSONObject.parseObject(value, SslAccessLog.class);
                    IdentityEvent identityEvent = new IdentityEvent();
                    String identity = sslAccessLog.getGN().substring(0, sslAccessLog.getGN().indexOf('|'));
                    identityEvent.setIdentity(identity);
                    identityEvent.setTime(sslAccessLog.getReceiveTime());
                    return identityEvent;
                })
                .returns(IdentityEvent.class);

        Pattern<IdentityEvent, IdentityEvent> pattern = Pattern
                .<IdentityEvent>begin("start")
                .where(new IterativeCondition<IdentityEvent>() {
                    // 判断是否将当前事件添加到 start 事件中
                    @Override
                    public boolean filter(IdentityEvent value, Context<IdentityEvent> ctx) throws Exception {
                        boolean notAdd = true;
                        for (IdentityEvent startEvent : ctx.getEventsForPattern("start")) {
                            if (value.getIdentity().equals(startEvent.getIdentity())) {
                                notAdd = false;
                                break;
                            }
                        }
                        return notAdd;
                    }
                })
                .within(Time.seconds(15));


        CEP
                .pattern(stream, pattern)
                .process(new PatternProcessFunction<IdentityEvent, IdentityEvent>() {
                    @Override
                    public void processMatch(Map<String, List<IdentityEvent>> match, Context ctx, Collector<IdentityEvent> out) throws Exception {
                        List<IdentityEvent> eventList = match.get("start");
                        out.collect(eventList.get(eventList.size()));
                    }
                })
                .print();


        env.execute("test");


    }

}
