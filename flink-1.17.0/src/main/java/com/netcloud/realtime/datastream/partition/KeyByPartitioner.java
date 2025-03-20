package com.netcloud.realtime.datastream.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author netcloud
 * @date 2025-02-26 15:29:25
 * @email netcloudtec@163.com
 * @description
 */
public class KeyByPartitioner {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("A", 1), Tuple2.of("B", 2), Tuple2.of("A", 3), Tuple2.of("B", 4)
        );
        stream.keyBy(value -> value.f0).print("广播分区后输出:");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
