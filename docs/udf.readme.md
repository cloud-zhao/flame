## 1. 简介

实时任务里，对于复杂的逻辑计算，预计都会通过 udf 实现，这里用于介绍目前已经实现的 udf.

## 2. 注册方式

1. 按照 [Flink User-defined Functions](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/functions/systemFunctions.html)实现 UDF  
2. 继承`UdfFactory`实现`registr`：通过`tableEnv.registerFunction`注册进来
3. 注册的函数名通过`flame.udf.???.name`配置指定 
4. 实现UdfFactory之后在配置中设置 flame.udfs = ${className}. 如有多个类用逗号分割. 并将自己类所在的jar包放到工作目录的lib目录下
5. 配置示例: flame.udfs = com.zyb.rtsql.TestUdfFactory,com.homework.da.TsUdfFactory

## 3. Common UDF

Common用于实现通用UDF，由于 [flink 内置函数](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/functions/systemFunctions.html)不断增加，为了避免未来名字冲突，通用 udf 命名，统一加上`z_`前缀用于区分.

### 3.1. z_json_extract

用于解析 json 字符串，例如

```
("{\"name\": \"Jeff Dean\", \"address\": {\"country\": \"AM\", \"company\": \"G\"}}", "$.name")
# 多级解析
("{\"name\": \"Jeff Dean\", \"address\": {\"country\": \"AM\", \"company\": \"G\"}}", "$.address.country")
```


