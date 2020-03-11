package org.flame.streaming.config

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.flame.streaming.Logging

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by cloud on 2020/03/03.
  */
class FlameConfig(properties: Properties) extends Logging with Serializable {

  private val settings: ConcurrentHashMap[String, String] = loadProperties
  private var tableConfig: TableConfig = _
  private val commonUdfArray = Array(                  // 默认UDF列表
    "org.flame.streaming.udf.CommonUdfFactory"       // 通用UDF
  )

  private def loadProperties: ConcurrentHashMap[String, String] = {
    val _settings = new ConcurrentHashMap[String, String]()
    properties.stringPropertyNames()
      .filter(k => k.startsWith("flame."))
      .foreach(k => _settings.put(k, properties.getProperty(k)))
    _settings
  }

  def setJobName(name: String): FlameConfig = {
    set("flame.job.name", name)
  }

  def setMetadataUrl(url: String): FlameConfig = {
    set("flame.metadata.url", url)
  }

  def set(key: String, value: String): FlameConfig = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  def getPolicy: String = get("flame.policy", "org.flame.streaming.policy.SimpleOrderPolicy")

  def getJobName: String = get("flame.job.name")

  def getMetadataUrl: String = {
    get("flame.metadata.url")
  }

  def getTableConfig: TableConfig = {
    if (tableConfig == null) {
      tableConfig = new TableConfig(this)
      tableConfig
    } else tableConfig
  }

  def getUdfList: Array[String] = {
    val udfArray = get("flame.udfs","").split(",").filter("" !=).map(_.trim)
    udfArray ++ commonUdfArray
  }

  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

}

object FlameConfig {

  private var flameConfig: FlameConfig = _

  def getConfig: Option[FlameConfig] = Option(flameConfig)

  def apply(properties: Properties): FlameConfig = {
    if (flameConfig == null) {
      flameConfig = new FlameConfig(properties)
      flameConfig
    } else flameConfig
  }


  def parseFile(path: String): FlameConfig = {
    if (flameConfig == null) {
      val properties = new Properties()
      properties.load(new FileInputStream(path))
      apply(properties)
    } else flameConfig
  }

}
