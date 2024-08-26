package com.netcloud.realtime;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用的Maven依赖，可以解决Flink1.14版本不兼容问题
 *<dependency>
 *     <groupId>com.ververica</groupId>
 *     <artifactId>flink-sql-connector-mysql-cdc</artifactId>
 *     <version>2.2.1</version>
 * </dependency>
 *
 * <dependency>
 *     <groupId>com.ververica</groupId>
 *     <artifactId>flink-connector-mysql-cdc</artifactId>
 *     <version>2.2.1</version>
 * </dependency>
 */

public class Flink_CDC_Mysql_V1 {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 增加无锁配置
        //Properties prop = new Properties();
        //prop.setProperty("debezium.snapshot.locking.mode", "none");
        //2.通过FlinkCDC构建SourceFunction并读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder().
                hostname("172.16.240.21")
                .port(3306)
                .username("root")
                .password("Sunmnet@123")
                .databaseList("test")
                //配置目前用到的表
                .tableList("test.test1")
                //.deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(new CustomerDeserialization())
                //.debeziumProperties(prop)
                //从指定文件的指定位置同步，通过mysql> SHOW MASTER STATUS;查看
                .startupOptions(StartupOptions.latest())
                .build();
        DataStream<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"mysqlSource");
        streamSource.print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

