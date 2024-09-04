package com.netcloud.realtime;

import com.netcloud.realtime.util.CustomerDeserialization;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;

/**
 * 版本匹配关系：https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/overview/
 * Flink1.20 FlinkCDCVersion 3.1.1
 * 这里使用的依赖如下
 * <dependency>
 *     <groupId>org.apache.flink</groupId>
 *     <artifactId>flink-connector-mysql-cdc</artifactId>
 *     <version>3.1.1</version>
 * </dependency>
 */
public class Flink_CDC_Mysql_V1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("172.16.240.21")
                .port(3306)
                .username("root")
                .password("Sunmnet@123")
                .databaseList("test")
                .tableList("test.test1,test.test2")
                // converts SourceRecord to JSON String
                //.deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(new CustomerDeserialization())//自定义反序列化
                //从指定文件的指定位置同步，通过mysql> SHOW MASTER STATUS;查看
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        mySQLSource.print();
        try {
            env.execute("Print MySQL Snapshot + Binlog");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

