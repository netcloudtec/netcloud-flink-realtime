package com.netcloud.realtime.datastream.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author netcloud
 * @date 2025-02-26 15:38:33
 * @email netcloudtec@163.com
 * @description
 */
public class CustomPartition {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("A", 1), Tuple2.of("B", 2), Tuple2.of("A", 3), Tuple2.of("B", 4)
        );
        stream.print("源数据输出:");
        stream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
//                return key.hashCode() % numPartitions;  // 取模分区
                if(key.equals("A")) {
                    return 1;
                }else if(key.equals("B")) {
                    return 2;
                }
                return 3;
            }
        }, value -> value.f0).print("自定义分区输出:");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
