package com.netcloud.realtime;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;


/**
 * 官网案例：信用卡交易的诈骗检测
 * （使用DataStream API自己去维护状态和time达到状态和时间的细粒度控制）
 * 官网在此版本已经弃用此案例API，这里学习思想。
 * ValueState 是一种键控状态，它是跟在keyBy 后面的运算符。
 * 欺诈交易的规则: 在一分钟内，连续交易中，上笔交易是小额交易，下一笔是大额交易
 */
public class FraudDetectionJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

        try {
            env.execute("Fraud Detection");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
