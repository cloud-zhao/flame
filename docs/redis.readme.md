## 1. DDL 样例

create table tblZyRedis (
 `user_id` VARCHAR,
 `cnt` INT
 ) WITH (
 'update-mode' = 'append',
 'connector.type' = 'redis',
 'connector.bns' = '',
 'connector.password' = '',
 'connector.filter' = 'false',
 'connector.table' = 'tblZyRedis',
 'connector.format' = 'hash',
 'connector.number' = '1'
)


## 2. 配置说明

| 字段 | 取值 | 说明 |
| --- | --- | --- |
| bns | string | codis-proxy 的 bns 地址 |
| password | string | coids-proxy password |
| table | string | 表名，定义后不可修改 |
| format | hash | 格式，定义后不可修改 |
| number | int | 打散区间，定义后不可修改 |
