package org.flame.streaming.udf

import org.flame.streaming.Logging
import org.flame.streaming.config.FlameConfig
import org.flame.streaming.util.JsonUtil
import org.apache.flink.table.functions.ScalarFunction

import scala.util.{Failure, Success, Try}

/**
  * Created by cloud on 2020/02/21.
  */
class CommonUdfFactory(val flameConfig: FlameConfig) extends UdfFactory {
  override def register(): Unit = {
    val jsonExtractName = flameConfig.get("flame.udf.jsonExtract.name", "z_json_extract")
    tableEnv.registerFunction(jsonExtractName, new JsonExtractFunction)
  }
}

/**
  * @Description: Json解析Udf，功能：
  *              1. json = {"name": "Jeff Dean", "address": {"country": "AM"}}, path = "$.address.country"
  */
class JsonExtractFunction extends ScalarFunction with Logging {
  def eval(json: String, path: String): String = {
    Try {
      logInfo(s"eval json: $json path: $path")
      val map = JsonUtil.toAny[Map[String, Any]](json)
      val keys = path.split("\\.")
      getValue(keys.tail, map)
    } match {
      case Success(v) => v
      case Failure(e) => throw e
    }
  }

  private def getValue(keys: Array[String], value: Map[String, Any]): String = {
    if (keys.nonEmpty) {
      Try(value(keys.head)) match {
        case Success(v: Map[String, Any]) => getValue(keys.tail, v)
        case Success(v) if keys.tail.isEmpty => v.toString
        case Success(v) => throw new UnsupportedOperationException(s"${keys.tail.mkString(".")}, $v")
        case Failure(e) => throw e
      }
    } else value.toString()
  }
}

object Txx {

  def main(args: Array[String]): Unit = {
    val json ="""{"name": "Jeff Dean", "address": {"country": "AM","test2":[1,2,3,4],
        |"test1":{"cs":{"fs":200,"ns":{"a":1,"b":2}}}}}""".stripMargin
    val p1 = "$.address.test2.0"
    val p2 = "$.address.test1.cs.fs"
    val p3 = "$.address.test1.cs.ns"
    val map = JsonUtil.toAny[Map[String, Any]](json)

    println(getValue(p1.split("\\.").tail, map))
    println(getValue(p2.split("\\.").tail, map))
    println(getValue(p3.split("\\.").tail, map))

  }

  private def getValue(keys: Array[String], value: Map[String, Any]): String = {
    if (keys.nonEmpty) {
      Try(value(keys.head)) match {
        case Success(v: Map[String, Any]) => getValue(keys.tail, v)
        case Success(v) if keys.tail.isEmpty => v.toString
        case Success(v) => throw new UnsupportedOperationException(s"${keys.tail.mkString(".")}, $v")
        case Failure(e) => throw e
      }
    } else value.toString()
  }
}
