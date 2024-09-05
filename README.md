# 模块功能

在使用不同版本的 Flink 时，可能会遇到 MySQL CDC（Change Data Capture）连接器的兼容性问题。以下是不同版本的 MySQL CDC 连接器与 Flink 的兼容性说明：

- **Flink 1.14 版本**使用以下依赖会导致冲突：
    ```xml
    <dependency>
        <groupId>com.alibaba.ververica</groupId>
        <artifactId>flink-connector-mysql-cdc</artifactId>
        <version>1.2.0</version>
    </dependency>
    ```

- **Flink 1.14 版本**使用以下依赖时异常退出，该版本仅支持 Flink 1.19 以上版本：
    ```xml
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-mysql-cdc</artifactId>
        <version>3.1.1</version>
    </dependency>
    ```

- **Flink 1.14 版本**使用以下依赖能够解决问题：
    ```xml
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
    ```

**注：** `netcloud-flink-cdc-mysqlv1` 与 `Flink 1.14.5` 兼容。
