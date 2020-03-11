package org.flame.streaming.table.source

import org.flame.streaming.Logging
import org.flame.streaming.util.{CodisUtil, JsonUtil}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.flink.types.Row

/**
  * Created by cloud on 2020/02/19.
  */
class RedisTableFunction(redisConfig: RedisConfig,
                         fields: Array[String],
                         fieldTypes: Array[TypeInformation[_]],
                         lookupKeys: Array[String]) extends TableFunction[Row] with Logging {

  private val codisEndPoint = redisConfig.endpoint
  private val resultType = new RowTypeInfo(fieldTypes, fields)

  override def getResultType: TypeInformation[Row] = resultType

  override def open(context: FunctionContext): Unit = {
    CodisUtil.safeClose(redis => redis.isConnected)(CodisUtil.connect(codisEndPoint))
  }

  override def close(): Unit = {
    CodisUtil.close(codisEndPoint)
  }

  @scala.annotation.varargs
  def eval(keys: AnyRef*): Unit = {
    //    当前大部分redis维表写入key时，没有打散，为了兼容当前已经写入的维表格式，如果 tableMaxNumber = 1，则不加_后缀
    val table = if (redisConfig.tableMaxNumber == 1) {
      redisConfig.tableName
    } else {
      s"${redisConfig.tableName}_${(keys(0).hashCode() & 0x7FFFFFFF) % redisConfig.tableMaxNumber}"
    }
    val key = s"${keys(0)}"
    CodisUtil.safeClose { redis =>
      val v = redis.hget(table, key)
      val row = new Row(fields.length)
      if (v != null) {
        val map = JsonUtil.toAny[Map[String, Any]](v)
        fields.zipWithIndex.foreach { case (k, i) => row.setField(i, map2FieldType(i, map(k))) }
      } else {
        val warningMsg = s"hget $table $key got nothing"
        logWarning(warningMsg)
        throw new NullPointerException(warningMsg)
      }
      collect(row)
    }(CodisUtil.connect(codisEndPoint))
  }

  /**
    * @Description: 对于数字类型，json4s 内部使用 JInt 表示，转换为 scala 类型则对应 scala.math.BigInt，
    *               如果该字段在 tableSchema 里定义为 Int，就会产生类型 cast 错误。
    *               因此通过 tableSchema 提前对取出的值强制转换后返回
    * @param i 第几列
    * @param a 对应的值
    */
  private def map2FieldType(i: Int, a: Any): Any = (fieldTypes(i), a) match {
    case (Types.LONG, value: BigInt) => value.toLong
    case (Types.BIG_INT, value: BigInt) => value.toLong
    case (Types.INT, value: BigInt) => value.toInt
    case _ => a
  }
}
