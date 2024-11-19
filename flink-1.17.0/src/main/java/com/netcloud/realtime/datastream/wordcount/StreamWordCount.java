package com.netcloud.realtime.datastream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author netcloud
 * @date 2024-11-20 00:17:08
 * @email netcloudtec@163.com
 * @description
 */
public class StreamWordCount {
    public static void main(String[] args) {
        //TODO 1 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //TODO 2 source: 从socket文本流读取数据 加载为DataStream
        DataStream<String> inputDataStream = env.socketTextStream(host, port);
        //TODO 3 transformation:基于数据流进行转换计算 每行数据根据指定分隔符拆分为单词 返回一个Tuple2二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //TODO 4 进行单词统计 key(0) 这里的0表示Tuple的第一个位置；此方式被弃用了；使用 keyBy(tuple2 -> tuple2.f0).sum(1)
        //sum(1) 表示元组的第二个位置
        DataStream<Tuple2<String, Integer>> resultStream = flatMapStream.keyBy(tuple2 -> tuple2.f0).sum(1);
        //TODO 5 sink
        resultStream.print();
        //TODO 6 execute/启动并等待程序结束
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
