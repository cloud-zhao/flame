## 1. DDL 样例

kafka Binlog 表：

```
CREATE TABLE tblNewTradeOriginal (
  BINLOG_NAME VARCHAR,
  BINLOG_POS DECIMAL,
  `DATABASE` VARCHAR,
  `TABLE` VARCHAR,
  `GLOBAL_ID` VARCHAR,
  `NEW_VALUES` ROW<`id` STRING, `user_id` STRING, `tcc_id` STRING, `trade_id` STRING, `order_id` STRING, `sub_order_group` STRING, `biz_info_group` STRING, `deduct_info_group` STRING, `amount_payable` STRING, `amount_discount` STRING, `amount_transfer` STRING, `amount_pretrade` STRING, `amount_prediscount` STRING, `amount_trade` STRING, `pay_map` STRING, `trade_time` STRING, `payment_channel` STRING, `payment_discount` STRING, `status` STRING, `log_info` STRING, `order_type` STRING, `business_type` STRING, `create_time` STRING, `update_time` STRING, `ext_flag` STRING, `ext_data` STRING, `order_channel` STRING, `review_status` STRING>,
  `TYPE` VARCHAR
  ) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = '???',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = '???',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = '???',
  'connector.properties.2.key' = 'group.id',
  'connector.properties.2.value' = '???',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true')
```

kafka 普通表：

```
CREATE TABLE tblUser (
  id VARCHAR,
  user_id VARCHAR,
  trade_id VARCHAR
  ) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = '???',
  'connector.properties.0.key' = 'zookeeper.connect',
  'connector.properties.0.value' = '???',
  'connector.properties.1.key' = 'bootstrap.servers',
  'connector.properties.1.value' = '???',
  'update-mode' = 'append',
  'format.type' = 'json',
  'format.derive-schema' = 'true')
```
