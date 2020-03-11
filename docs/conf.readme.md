
## 1. 启动

启动前需设置环境变量：

```
1. export FLINK_HOME=
```

## 2. 配置

### 2.1. properties

| 字段 | 取值 | 说明 |
| --- | --- | --- |
| 运行 |
| flame.run.parallelism | int | 任务并行度 |
| flame.run.queue | string(default=default) | 任务队列 |
| 任务 |
| flame.job.name | string  | 任务名，格式要求为$user@$desc@$tag@$version,例如user1@kafka_to_kafka@demo@1.0 |
| flame.tables | string | 所有相关的表名称，','分隔 |
| flame.table.xxx.type | factory/query | xxx必须在 flink.tables 里已定义 1. factory:适用 DDL/INSERT INTO语句 <br> 2. query:适用 SELECT 语句，结果表名即xxx|
| flame.table.xxx.sql | string | DDL/SQL |
| flame.table.xxx.remote | bool | 创建表时，是否从元数据平台获取 DDL. <br>使用此选项时必须设置可用的flame.metadata.url <br> 注：优先使用 flame.table.xxx.sql |

### 2.2. builtin properties

| 字段 | 取值 | 说明 |
| --- | --- | --- |
| 运行 |
| flame.run.slot |
| flame.run.main.class |
| flame.run.main.jar |
| flame.run.job-manager-mem |
| flame.run.task-manager-mem |
| 任务 |
| flame.metadata.url | string(default=http://flink.apache.org) | 元数据平台地址, 目前只支持http通信 |

### 2.3. DDL

1. [Kafka](./kafka.readme.md)
2. [Redis](./redis.readme.md)


## 4. FAQ

1. Binlog

Binlog 在表格式的基础上增加了一层数据，例如 BINLOG_NAME BINLOG_POS DATABASE TABLE 等，NEW_VALUES/OLD_VALUES 对应的才是 mysql 的表数据。如果表的 DDL 需要从元数据平台获取，那么默认会额外生成一张 Binlog 的表。如果表的 DDL 定义在 properties，需要在 sql 手动导出 NEW_VALUES 对应的表。