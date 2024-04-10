package com.cnso.flinkcdc.common.constant;

public class ParameterConstant {

    // flink checkpoint hdfs地址
    public static final String FLINK_CHECKPOINT_DIR = "flink.checkpoint.dir";

    // mysql-source相关配置信息
    public static final String START_OPTION = "startup.option";
    public static final String START_OPTION_TIMESTAMP = "startup.option.timestamp";
    public static final String START_OFFSET_FILE = "startup.offset.file";
    public static final String START_OFFSET_POS = "startup.offset.pos";
    public static final String SOURCE_MYSQL_HOST = "source.mysql.host";
    public static final String SOURCE_MYSQL_PORT = "source.mysql.port";
    public static final String SOURCE_MYSQL_DATABASES = "source.mysql.databases";
    public static final String SOURCE_MYSQL_TABLES = "source.mysql.tables";
    public static final String SOURCE_MYSQL_USERNAME = "source.mysql.username";
    public static final String SOURCE_MYSQL_PASSWORD = "source.mysql.password";
    public static final String SOURCE_MYSQL_SERVER_ID = "source.mysql.server.id";

    // hdfs输出目录
    public static final String OUTPUT_PATH = "output.path";

    // kafka相关配置
    public static final String SOURCE_KAFKA_BOOTSTRAP_SERVERS = "source.kafka.BootstrapServers";
    public static final String SOURCE_KAFKA_TOPICS = "source.kafka.topics";
    public static final String SOURCE_KAFKA_GROUP_ID = "source.kafka.group.id";
    public static final String SOURCE_KAFKA_USERNAME = "source.kafka.username";
    public static final String SOURCE_KAFKA_PASSWORD = "source.kafka.password";

    // tidb相关配置
    public static final String SINK_TIDB_DRIVER = "sink.tidb.driver";
    public static final String SINK_TIDB_URL = "sink.tidb.url";
    public static final String SINK_TIDB_USERNAME = "sink.tidb.username";
    public static final String SINK_TIDB_PASSWORD = "sink.tidb.password";
    public static final String SINK_TIDB_DATABASE = "sink.tidb.database";

    // url
    public static final String IS_SEND_OFFSET = "is.send.offset";
    public static final String SEND_OFFSET_URL = "send.offset.url";

    public static final String WINDOW_TIME_SECONDS = "window.time.seconds";
    public static final String FILTER_KAFKA_TOPICS = "filter.kafka.topics";

}
