package org.flame.streaming.table.source

import java.lang.{Boolean => JBoolean, String => JString}
import java.util.{ArrayList => Jal, HashMap => HMap, List => JList, Map => JMap}

import org.flame.streaming.table.sink.RedisSink
import org.flame.streaming.util.CodisEndPoint
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.descriptors.Schema.{SCHEMA, SCHEMA_NAME, SCHEMA_TYPE}
import org.apache.flink.table.descriptors.{DescriptorProperties, SchemaValidator}
import org.apache.flink.table.factories.{StreamTableSinkFactory, StreamTableSourceFactory}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

/**
  * Created by cloud on 2020/02/19.
  */
class RedisTableFactory extends StreamTableSourceFactory[Row] with StreamTableSinkFactory[Tuple2[JBoolean, Row]] {

  import RedisConfig._

  override def requiredContext(): JMap[String, String] = {
    val map = new HMap[String, String]()
    map.put("update-mode", "append")
    map.put("connector.type", "redis")
    map
  }

  override def supportedProperties(): JList[String] = {
    val prop = new Jal[String]()

    /* redis config */
    prop.add(BNS)
    prop.add(PASSWORD)
    prop.add(FILTER)
    prop.add(TABLE_NAME)
    prop.add(TABLE_FORMAT)
    prop.add(TABLE_MAX_NUMBER)

    /* schema config */
    prop.add(s"$SCHEMA.#.$SCHEMA_NAME")
    prop.add(s"$SCHEMA.#.$SCHEMA_TYPE")

    prop
  }

  override def createStreamTableSource(properties: JMap[JString, JString]): StreamTableSource[Row] = {
    val desc = getDesc(properties)
    new RedisSource(RedisConfig(desc), desc.getTableSchema(SCHEMA))
  }

  override def createStreamTableSink(properties: JMap[JString, JString]): StreamTableSink[Tuple2[JBoolean, Row]] = {
    val desc = getDesc(properties)
    new RedisSink(RedisConfig(desc), desc.getTableSchema(SCHEMA))
  }

  private def getDesc(prop: JMap[JString, JString]): DescriptorProperties = {
    val desc = new DescriptorProperties(true)
    desc.putProperties(prop)
    new SchemaValidator(true, false, false).validate(desc)
    desc
  }

}

sealed trait RedisFormat
case object HashFormat extends RedisFormat
case object StringFormat extends RedisFormat

/**
  * redis table 需要的相关配置
  * @param bnsName        bns名称
  * @param password       codis密码
  * @param filterStatus   是否过滤codis节点
  * @param tableName      redis是中表名称,当使用hash模式时此项为redis key，若使用字符串模式时此项为key的前缀
  * @param tableFormat    指定存储在redis中使用hash 存储还是普通字符串存储
  * @param tableMaxNumber 指定存储在redis中是否按此编号打散存储
  */
case class RedisConfig(bnsName: String,
                       password: String,
                       filterStatus: Boolean,
                       tableName: String,
                       tableFormat: RedisFormat,
                       tableMaxNumber: Int) extends Serializable {

  def endpoint: CodisEndPoint = CodisEndPoint(bnsName, password, filterStatus)

}

object RedisConfig {
  val BNS = "connector.bns"
  val PASSWORD = "connector.password"
  val FILTER = "connector.filter"
  val TABLE_NAME = "connector.table"
  val TABLE_FORMAT = "connector.format"
  val TABLE_MAX_NUMBER = "connector.number"

  def apply(desc: DescriptorProperties): RedisConfig = {
    val bnsName = desc.getString(BNS)
    val password = desc.getString(PASSWORD)
    val filterStatus = desc.getBoolean(FILTER)
    val tableName = desc.getString(TABLE_NAME)
    val tableFormat = desc.getString(TABLE_FORMAT).toLowerCase match {
      case "hash" => HashFormat
      case _ => StringFormat
    }
    val tableMaxNumber = desc.getInt(TABLE_MAX_NUMBER)
    RedisConfig(bnsName, password, filterStatus, tableName, tableFormat, tableMaxNumber)
  }
}
