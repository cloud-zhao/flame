package org.flame.streaming.env

import org.flame.streaming.config.FlameConfig
import org.flame.streaming.util.SQLUtil
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Created by cloud on 2020/02/21.
  */
object SQLEnv {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  private val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  private val tableEnv = StreamTableEnvironment.create(env, setting)

  def getEnv: StreamExecutionEnvironment = env

  def getTableEnv: StreamTableEnvironment = tableEnv

  /**
    * @Description: 调用传入的 ddl，创建 SQL 环境下的table
    * @Param:
    * @param ddlList 传入的ddl
    * @return: void
    */
  def createTables(ddlList: Iterable[String]): Unit = {
    for (ddl <- ddlList) {
      println(ddl)
      tableEnv.sqlUpdate(ddl)
    }
  }

  /**
    * @Description: 执行 sql 并将结果转化为一个新的table
    * @Param:
    * @param sql       待执行sql
    * @param tableName 转话的表名
    * @return: void
    */
  def sqlResultAsTable(sql: String, tableName: String): Unit = {
    require(SQLUtil.isSelect(sql), "only SELECT result could transform to a table.")
    tableEnv.registerTable(tableName, tableEnv.sqlQuery(sql))
  }

  // 执行 sql，还需结合策略框架修改
  def runQuery(sql: String): Unit = {
    if (SQLUtil.isSelect(sql)) {
      tableEnv.sqlQuery(sql).toRetractStream[Row].addSink(new SinkFunction[(Boolean, Row)] {
        override def invoke(value: (Boolean, _root_.org.apache.flink.types.Row)): Unit = {
          if (value._1) println(value._2)
        }
      })
    } else if (SQLUtil.isInsert(sql)) {
      tableEnv.sqlUpdate(sql)
    }
  }

  def execute(): JobExecutionResult = execute(FlameConfig.getConfig.get.getJobName)

  def execute(name: String): JobExecutionResult = env.execute(name)

}
