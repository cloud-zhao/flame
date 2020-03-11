package org.flame.streaming.table.sink

import org.flame.streaming.table.source.RedisConfig
import org.flame.streaming.util.{CodisUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import scala.collection.mutable.{Map => MMap}

/**
  * Created by cloud on 2020/02/17.
  */
class RedisSink(redisConfig: RedisConfig,
                schema: TableSchema) extends UpsertStreamTableSink[Row] {

  private val tableSchema = schema
  private var indices: Array[String] = _
  private var appendOnly: Boolean = false

  override def setKeyFields(strings: Array[String]): Unit = {
    indices = strings
  }

  override def setIsAppendOnly(aBoolean: java.lang.Boolean): Unit = {
    appendOnly = aBoolean
  }

  override def getRecordType: TypeInformation[Row] = tableSchema.toRowType

  override def getTableSchema: TableSchema = tableSchema

  override def getFieldNames: Array[String] = tableSchema.getFieldNames

  override def getFieldTypes: Array[TypeInformation[_]] = TypeConversions.fromDataTypeToLegacyInfo(tableSchema.getFieldDataTypes)

  override def configure(fieldNames: Array[String],
                         fieldTypes: Array[TypeInformation[_]]): TableSink[Tuple2[java.lang.Boolean, Row]] = {
    val copy = new RedisSink(redisConfig, tableSchema)
    copy.setKeyFields(indices)
    copy
  }

  override def emitDataStream(dataStream: DataStream[Tuple2[java.lang.Boolean, Row]]): Unit = {
    dataStream.addSink(new RedisFunction(redisConfig, tableSchema.getFieldNames)).name("redis function")
  }

  override def consumeDataStream(dataStream: DataStream[Tuple2[java.lang.Boolean, Row]]): DataStreamSink[_] = {
    dataStream.addSink(new RedisFunction(redisConfig, tableSchema.getFieldNames)).name("redis function")
  }

}

class RedisFunction(redisConfig: RedisConfig,
                    fields: Array[String]) extends RichSinkFunction[Tuple2[java.lang.Boolean, Row]] {

  private val codisEndpoint = redisConfig.endpoint

  override def open(parameters: Configuration): Unit = {
    CodisUtil.safeClose(redis => redis.isConnected )(CodisUtil.connect(codisEndpoint))
  }

  override def invoke(value: Tuple2[java.lang.Boolean, Row], context: SinkFunction.Context[_]): Unit = {
    val isDel = ! value.f0
    val row = value.f1
    CodisUtil.safeClose { redis =>
      val key = row.getField(0)
      //      当前大部分redis维表写入key时，没有打散，为了兼容当前已经写入的维表格式，如果 tableMaxNumber = 1，则不加_后缀
// TODO: 跟 RedisTableFunction 该逻辑以及生成 key 的代码合并
      val table = if (redisConfig.tableMaxNumber == 1) {
        redisConfig.tableName
      } else {
        s"${redisConfig.tableName}_${(key.hashCode() & 0x7FFFFFFF) % redisConfig.tableMaxNumber}"
      }
      val map = MMap.empty[String, AnyRef]
      fields.zipWithIndex.foreach { case (k, i) => map += k -> row.getField(i) }
      val value = JsonUtil.toJson(map.toMap)
      if (isDel) redis.hdel(table, key.toString) else redis.hset(table, key.toString, value)
    }(CodisUtil.connect(codisEndpoint))
  }

  override def close(): Unit = {
    CodisUtil.close(codisEndpoint)
  }
}
