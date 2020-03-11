package org.flame.streaming.metadata

import java.net.{ConnectException, SocketTimeoutException}
import java.util.concurrent.ConcurrentHashMap

import org.flame.streaming.Logging
import org.flame.streaming.util.{HttpUtil, JsonUtil}

import scala.util.{Failure, Success, Try}

/**
  * Created by cloud on 2020/03/03.
  */
class MetaDataClient(url: String,
                     maxRetryTimes: Int,
                     retryIntervalMs: Int) extends Logging {

  private val platformUrl: String = check_url
  private val cache: ConcurrentHashMap[String, Metadata] = new ConcurrentHashMap[String, Metadata]()

  def check_url: String = {
    val _url = if (url.startsWith("http://") || url.startsWith("https://")) url else ""
    require(!_url.isEmpty, "metadata url not set.")
    _url
  }

  def fetchMetadata(db: String, table: String): Metadata = {
    val key = s"$db.$table"
    if (cache.containsKey(key)) cache.get(key) else {
      val md = fetchMetadata(s"""{"database":"$db","table":"$table","env":"online"}""")
      cache.put(key, md)
    }
  }

  /**
    * 获取元数据
    * @param data json格式的请求体{"database":"","table":"","env":""}
    * @return
    */
  def fetchMetadata(data: String, attempt: Int = 0): Metadata = {
    if (attempt < maxRetryTimes) {
      Try {
        HttpUtil.postJsonData(platformUrl, data)
      } match {
        case Success(value) if value.code == 200 =>
          val md = JsonUtil.toAny[Metadata](value.body)
          if (md.isSuccess) md else {
            Thread.sleep(retryIntervalMs * (attempt + 1))
            fetchMetadata(data, attempt + 1)
          }
        case Success(_) => fetchMetadata(data, attempt + 1)
        case Failure(_: SocketTimeoutException) => fetchMetadata(data, attempt + 1)
        case Failure(e) => throw e
      }
    } else throw new ConnectException(s"fetch metadata failed. attempt $attempt, max retry times $maxRetryTimes")
  }

}

object MetaDataClient {

  private var client: MetaDataClient = _

  def apply(url: String): MetaDataClient = {
    if (client != null) client else {
      client = new MetaDataClient(url, 3, 50)
      client
    }
  }

}

/**
  * 元数据平台例存储的表字段信息
  * @param name     字段名
  * @param `type`   字段类型,此次为sql类型, 例: VARCHAR
  * @param comment  字段描述
  */
case class MetadataField(name: String,
                         `type`: String,
                         comment: String) {
  def toSqlString: String = s"""`$name` ${`type`}"""
}

/**
  * 元数据配置字段信息和其他参数信息
  * @param fields   表字段信息列表
  * @param storage  通用参数列表,目前尚未用到
  */
case class MetadataInfo(fields: List[MetadataField],
                        storage: Map[String, Any]) {
  def toSqlString: String = fields.map(_.toSqlString).mkString("(",",",")")
}

/**
  * 元数据平台返回的数据结构
  * @param version    数据版本
  * @param code       结果状态，0为正常
  * @param msg        结果信息
  * @param env        环境信息,目前暂时用不到
  * @param database   库名
  * @param table      表名
  * @param info       字段信息和额外配置信息
  */
case class Metadata(version: Int,
                    code: Int,
                    msg: String,
                    env: String,
                    database: String,
                    table: String,
                    info: MetadataInfo) {

  def isSuccess: Boolean = code == 0

  def toSqlString: String = s"create table $table ${info.toSqlString}"

}
