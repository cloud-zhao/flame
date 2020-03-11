package org.flame.streaming.util

/**
  * Created by cloud on 2020/02/21.
  */
object HttpUtil {

  case class SimpleHttpResponse(code: Int, body: String)

  /**
    * @Description: post方式提交json数据到指定的url，获取返回结果及返回码.连接及读取超时默认为1000ms/5000ms
    */
  def postJsonData(url: String,
                   data: String,
                   connTimeoutMs: Int = 1000,
                   readTimeoutMs: Int = 5000): SimpleHttpResponse = {
    val res = scalaj.http.Http(url)
      .timeout(connTimeoutMs, readTimeoutMs)
      .postData(data)
      .header("content-type", "application/json")
      .asString
    SimpleHttpResponse(res.code, res.body)
  }
}
