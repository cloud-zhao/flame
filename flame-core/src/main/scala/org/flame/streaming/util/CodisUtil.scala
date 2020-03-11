package org.flame.streaming.util

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.annotation.meta.getter
import scala.collection.JavaConversions._
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by cloud on 2020/02/21.
  */
object CodisUtil {
  lazy val logger = LoggerFactory.getLogger(getClass)

  @transient
  @getter
  private lazy val bns_pools: ConcurrentHashMap[String, JedisPool] =
    new ConcurrentHashMap[String, JedisPool]()

  /**
    * 创建或者获取一个Redis 连接池
    *
    * @param codisEndPoint
    * @return
    */
  def connect(codisEndPoint: CodisEndPoint): Jedis = {
    var pools = bns_pools.getOrElseUpdate(codisEndPoint.name, createJedisPool(codisEndPoint))
    var i = 0
    var sleepTime: Int = 50
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pools.getResource
      } catch {
        case x: JedisConnectionException => {
          if (sleepTime < 500) {
            sleepTime *= 2
            Thread.sleep(sleepTime)
          } else if (i < 3) {
            Try {
              pools.close()
            }
            pools = createJedisPool(codisEndPoint)
            i = i + 1
            bns_pools.put(codisEndPoint.name, pools)
          } else throw x
        }
        case e: Exception => throw e
      }
    }
    conn
  }

  /**
    * 创建一个连接池
    *
    * @param codisEndPoint
    * @return
    */
  def createJedisPool(codisEndPoint: CodisEndPoint): JedisPool = {

    logger.info(s"createJedisPool with ${codisEndPoint.name} ")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    /*最大连接数*/
    poolConfig.setMaxTotal(300)
    /*最大空闲连接数*/
    poolConfig.setMaxIdle(64)
    /*在获取连接的时候检查有效性, 默认false*/
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    /*在空闲时检查有效性, 默认false*/
    poolConfig.setTestWhileIdle(false)
    /*逐出连接的最小空闲时间 默认300000毫秒(5分钟)*/
    poolConfig.setMinEvictableIdleTimeMillis(300000)
    /*逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1*/
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    logger.info(s"from codis ${codisEndPoint.host}:${codisEndPoint.port} create redis pool")
    new JedisPool(poolConfig,
      codisEndPoint.host,
      codisEndPoint.port,
      Protocol.DEFAULT_TIMEOUT,
      codisEndPoint.password,
      Protocol.DEFAULT_DATABASE)
  }

  def safeClose[R](f: Jedis => R)(implicit jedis: Jedis): R = {
    val result = f(jedis)
    Try {
      jedis.close()
    } match {
      case Success(_) => logger.debug("jedis.close successful.")
      case Failure(_) => logger.error("jedis.close failed.")
    }
    result
  }

  def safeClosePipe[R](f: Pipeline => R)(implicit jedis: Jedis): R = {
    val pipe = jedis.pipelined()
    val result = f(pipe)
    Try {
      pipe.sync()
      pipe.close()
      jedis.close()
    } match {
      case Success(_) => logger.debug("pipe.close successful.")
      case Failure(_) => logger.error("pipe.close failed.")
    }
    result
  }

  def close(endPoint: CodisEndPoint): Unit = {
    bns_pools.filter(x => x._1 == endPoint.name).foreach(_._2.close())
  }

  def close(): Unit = {
    bns_pools.foreach { case (_, pool) => pool.close() }
    bns_pools.clear()
  }
}

case class CodisEndPoint(host: String,
                         port: Int,
                         password: String = null) extends Serializable {
  def name: String = s"$host:$port"
}
