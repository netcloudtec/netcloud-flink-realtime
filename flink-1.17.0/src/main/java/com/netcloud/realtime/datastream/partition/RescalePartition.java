package com.netcloud.realtime.datastream.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author netcloud
 * @date 2025-02-26 15:46:01
 * @email netcloudtec@163.com
 * @description
 */
public class RescalePartition {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("A", "B", "C", "D");
        DataStream<String> mapDstream = stream.map(data -> {
            return data;
        }).setParallelism(1);
        mapDstream.print("源数据输出:");
        mapDstream.rescale().map(data -> {
            return data;
        }).setParallelism(4).print("重缩放分区后输出:");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
