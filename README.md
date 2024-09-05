## 模块功能
<!--Flink1.14版本使用此依赖导致冲突 -->
<!--        <dependency>-->
<!--            <groupId>com.alibaba.ververica</groupId>-->
<!--            <artifactId>flink-connector-mysql-cdc</artifactId>-->
<!--            <version>1.2.0</version>-->
<!--        </dependency>-->

<!--Flink1.14版本使用此依赖异常退出,此版本仅支持flink1.19以上版本 -->
<!--        <dependency>-->
<!--            <groupId>org.apache.flink</groupId>-->
<!--            <artifactId>flink-connector-mysql-cdc</artifactId>-->
<!--            <version>3.1.1</version>-->
<!--        </dependency>-->

<!--Flink1.14版本使用此依赖可解决问题 -->
<dependency>
    <groupId>com.ververica</groupId>
    <artifactId>flink-sql-connector-mysql-cdc</artifactId>
    <version>2.2.1</version>
</dependency>

<dependency>
    <groupId>com.ververica</groupId>
    <artifactId>flink-connector-mysql-cdc</artifactId>
    <version>2.2.1</version>
</dependency>
netcloud-flink-cdc-mysqlv1 Flink1.14.5
