package org.flame.streaming

import org.flame.streaming.util.CodisEndPoint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}

/**
  * Created by cloud on 2020/02/20.
  */
object CloudTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, setting)

    val fields: Array[String] = Array("user_id","trade_id","course_id")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.STRING(), Types.STRING())
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("alvin_test_res")
        .property("bootstrap.servers", "data-bigdata-12-28.bjcq.zybang.com:9092")
        .property("group.id", "z.cloud.flink.001")
        .startFromLatest()
    ).withFormat(
      new Json().failOnMissingField(true).deriveSchema()
    ).withSchema(
      new Schema()
        .field("DATABASE", Types.STRING())
        .field("TABLE", Types.STRING())
        .field("NEW_VALUES", Types.ROW(fields, fieldTypes))
        .field("proctime", Types.SQL_TIMESTAMP()).proctime()
    ).inAppendMode()
      .registerTableSource("trade_table")

    val cp = CodisEndPoint("codis-sandbox.data.bjcq", "bigdata123", false)

    val create_source =
      """
        |create table codis_table (
        | `user_id` VARCHAR,
        | `ds_count` INT
        | ) WITH (
        | 'update-mode' = 'append',
        | 'connector.type' = 'redis',
        | 'connector.bns' = 'codis-sandbox.data.bjcq',
        | 'connector.password' = 'bigdata123',
        | 'connector.filter' = 'false',
        | 'connector.table' = 'cloud_test_002',
        | 'connector.format' = 'hash',
        | 'connector.number' = '1'
        | )
      """.stripMargin

    val create_sink =
      """
        |create table redis_table (
        | `user_id` VARCHAR,
        | `ds_count` INT
        | ) WITH (
        | 'update-mode' = 'append',
        | 'connector.type' = 'redis',
        | 'connector.bns' = 'codis-sandbox.data.bjcq',
        | 'connector.password' = 'bigdata123',
        | 'connector.filter' = 'false',
        | 'connector.table' = 'cloud_test_001',
        | 'connector.format' = 'hash',
        | 'connector.number' = '1'
        | )
      """.stripMargin

    tableEnv.sqlUpdate(create_source)
    tableEnv.sqlUpdate(create_sink)

    val res = tableEnv.sqlQuery(
      """
        |select trade_table.user_id,  codis_table.ds_count
        |from trade_table
        |join codis_table FOR SYSTEM_TIME AS OF trade_table.proctime on trade_table.user_id = codis_table.user_id
      """.stripMargin)

    tableEnv.registerTable("res", res)

    tableEnv.sqlUpdate(
      """
        |insert into redis_table select * from res
      """.stripMargin)

    tableEnv.execute("cloud_test_job")

  }



}
