package org.flame.streaming.udf

import org.flame.streaming.config.FlameConfig
import org.flame.streaming.util.JsonUtil
import org.apache.flink.table.functions.ScalarFunction

/**
  * Created by cloud on 2020/03/04.
  */
class ObjectReadFunction extends ScalarFunction {

  def eval(json: String, key: String): String = {
    val map = JsonUtil.toAny[Map[String, Any]](json)
    map.getOrElse(key, "").toString
  }

}

class ObjectReadUdfFactory(val flameConfig: FlameConfig) extends UdfFactory {

  override def register(): Unit = {
    val funcName = flameConfig.get("flame.udf.ord.name", "map_read")
    val jr = new ObjectReadFunction()
    tableEnv.registerFunction(funcName, jr)
  }

}
