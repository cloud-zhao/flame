CREATE TABLE tblBinlogFirstLevel (
 `BINLOG_NAME` VARCHAR,
 `BINLOG_POS` DECIMAL,
 `DATABASE` VARCHAR,
 `TABLE` VARCHAR
) WITH (
 'connector.type' = 'kafka',
 'connector.version' = 'universal',
 'connector.topic' = 'SqlDest',
 'connector.properties.0.key' = 'zookeeper.connect',
 'connector.properties.0.value' = '10.63.100.31:2181,10.63.105.22:2181,10.63.106.18:2181',
 'connector.properties.1.key' = 'bootstrap.servers',
 'connector.properties.1.value' = '10.63.106.14:9092,10.63.105.22:9092,10.63.106.18:9092,10.63.100.31:9092,10.63.100.48:9092',
 'update-mode' = 'append',
 'format.type' = 'json',
 'format.derive-schema' = 'true'
)
