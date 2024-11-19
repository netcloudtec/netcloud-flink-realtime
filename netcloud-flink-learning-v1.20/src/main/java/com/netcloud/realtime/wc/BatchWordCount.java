package com.netcloud.realtime.wc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/8/7 21:12
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("./data/test.txt")).build();

        DataStream<String> fileStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

         DataStream<Tuple2<String, Integer>> flatMapStream = fileStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String content, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = content.split(" ");
                for (String word : words) {
                    // 这里的out是数据收集器，表示将处理的数据收集起来。
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        flatMapStream.keyBy(0).sum(1).print();
        flatMapStream.print();

        //TODO 4 进行单词统计 groupBy(0) 这里的0表示Tuple的第一个位置；sum(1) 表示元组的第二个位置
////        DataSet<Tuple2<String, Integer>> resultSet = flatMapDataSet.groupBy(0).sum(1);
//        //TODO 5 结果输出打印
//        resultSet.print();
    }
}
