package com.netcloud.realtime.datastream.partition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author netcloud
 * @date 2025-02-26 12:08:40
 * @email netcloudtec@163.com
 * @description Flink的全局分区
 * 在本地IDEA进行测试，Mac机器硬件配置：4核8线程，所以其默认并行度是8
 */
public class GlobalPartitioner {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("A", "B", "C", "D");
        int defaultParallelism = env.getParallelism(); //默认并行度8，和机器性能有关
        System.out.println("默认并行度:"+defaultParallelism);
        stream.print("源数据输出:");
        stream.global().print("全局分区后输出:");
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
