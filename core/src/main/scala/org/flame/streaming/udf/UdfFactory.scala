package org.flame.streaming.udf

import org.flame.streaming.config.FlameConfig
import org.flame.streaming.env.SQLEnv
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * Created by cloud on 2020/03/04.
  */
trait UdfFactory {

  val flameConfig: FlameConfig
  val tableEnv: StreamTableEnvironment = SQLEnv.getTableEnv

  def register(): Unit

}
