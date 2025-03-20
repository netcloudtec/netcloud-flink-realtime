package com.netcloud.realtime.datastream.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author netcloud
 * @date 2025-02-26 15:07:15
 * @email netcloudtec@163.com
 * @description
 */
public class ForwardPartitioner {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("A", "B", "C", "D");
//        DataStream<String> mapDstream = stream.map(data -> {
//            return data;
//        }).setParallelism(5);
//        mapDstream.print("源数据输出:");
//        mapDstream.forward().map(data -> {
//            return data;
//        }).setParallelism(5).print("Forward数据输出:");

        stream.map(value -> "Mapped: " + value)
                .forward()
                .filter(value -> value.contains("A"))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
