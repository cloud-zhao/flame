package org.flame.streaming.table.source

import org.flame.streaming.util.{CodisUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.sources.{LookupableTableSource, StreamTableSource}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * Created by cloud on 2020/02/18.
  */
class RedisSource(redisConfig: RedisConfig,
                  schema: TableSchema) extends StreamTableSource[Row] with LookupableTableSource[Row] {

  private val tableSchema: TableSchema = schema
  private val returnType: TypeInformation[Row] = tableSchema.toRowType

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val func = new RedisSourceFunction(redisConfig, tableSchema.getFieldNames)
    execEnv.addSource(func)
  }

  /**
    * LookupableTableSource 的接口方法
    * @param lookupKeys  join 时指定的字段名字
    *                    join xxx on xxx.id = table.id , xxx.ff = table.ff 此时lookupKeys = Array("id", "ff")
    * @return TableFunction
    */
  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[Row] = {
    new RedisTableFunction(redisConfig, tableSchema.getFieldNames, tableSchema.getFieldTypes, lookupKeys)
  }

  /**
    * LookupableTableSource 的接口方法
    * @param lookupKeys 同上
    * @return
    */
  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = {
    throw new UnsupportedOperationException()
  }

  /**
    * LookupableTableSource 的接口方法
    * @return
    */
  override def isAsyncEnabled: Boolean = false


  override def getReturnType: TypeInformation[Row] = returnType

  override def getTableSchema: TableSchema = tableSchema

}

class RedisSourceFunction(redisConfig: RedisConfig,
                          fields: Array[String]) extends SourceFunction[Row] {

  @volatile private var isRunning: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
    CodisUtil.safeClose { redis =>
      var isNew: Boolean = true
      while (isRunning) {
        if (isNew) {
          0.until(redisConfig.tableMaxNumber).foreach { index =>
            val value = redis.hgetAll(s"${redisConfig.tableName}_$index")
            value.foreach { case (_, v) =>
              val map = JsonUtil.toAny[Map[String, Any]](v)
              val row = new Row(fields.length)
              fields.zipWithIndex.foreach { case (k, i) => row.setField(i, map(k)) }
              sourceContext.collect(row)
            }
            isNew = false
          }
        }
      }
    }(CodisUtil.connect(redisConfig.endpoint))
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
