package org.flame.streaming.util

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by cloud on 17/12/21.
  */
object JsonUtil {
  implicit val formats = org.json4s.DefaultFormats

  def toAny[T : Manifest](s : String): T = parse(s).extract[T]

  def parseToJValue(s: String): JValue = parse(s)

  def toJson(o: Any): String = compact(toJValue(o))

  def toJValue(o: Any): JValue = Extraction.decompose(o)

  def main(args: Array[String]): Unit = {
    val source = """{ "some": "JSON source" }"""
    val jsonAst = toAny[Map[String,Any]](source)
    val newSource=Map("timestamp"->System.currentTimeMillis())++ jsonAst
    println(toJson(newSource))
  }

}


