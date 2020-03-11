package org.flame.streaming.util

import java.net.ConnectException

import com.baidu.noah.naming.{BNSClient, BNSInstance}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Project: bigdata-tools
  * Description: 
  * User: jiashaopan
  * Date: 2019-12-13
  */
object BnsClientProtos {
  private val logger = LoggerFactory.getLogger(getClass)

  private val CACHE_UPDATE_INTERVAL = 5000
  private val CACHE_CAPACITY = 100
  private val TIME_OUT_MILLIS = 3000

  val DEFAULT_STRATEGY = 0 // random

  {
    BNSClient.setCacheUpdateInterval(CACHE_UPDATE_INTERVAL)
    BNSClient.setCacheCapacity(CACHE_CAPACITY)
  }

  def setCacheUpdateInterval(cacheUpdateInterval: Int) = {
    val interval = if(cacheUpdateInterval <= 0) CACHE_UPDATE_INTERVAL else cacheUpdateInterval
    BNSClient.setCacheUpdateInterval(interval)
  }

  def setCacheCapacity(cacheCapacity: Int) = {
    val capacity = if(cacheCapacity <= 0) CACHE_CAPACITY else cacheCapacity
    BNSClient.setCacheCapacity(capacity)
  }

  def bnsClientJson(serviceName: String, valid: Boolean = true): String = JsonUtil.toJson(bnsClient(serviceName, valid))

  def bnsClient(serviceName: String,  valid: Boolean = true): BnsMessage = {
    val bnsInfo = getInstanceList(serviceName, valid)
    bnsInfo match {
      case Some(list) => BnsMessage(true, s"Get $serviceName Success", Some(list.map(toBnsServer)))
      case _ => BnsMessage(false, s"Get $serviceName Failed", None)
    }
  }

  def getInstance(serviceName: String, strategy: Int = DEFAULT_STRATEGY, valid: Boolean = true): BnsServer = {
    val bnsInfo = bnsClient(serviceName, valid)
    bnsInfo.data match {
      case Some(list) => if(list.nonEmpty) {
        strategy match {
          case DEFAULT_STRATEGY => list(Random.nextInt(list.length))
          case _ => throw new ConnectException("bns server strategy none")
        }
      } else throw new ConnectException("bns server empty")
      case _ => throw new ConnectException("bns server none")
    }
  }

  def getInstanceList(serviceName: String, valid: Boolean = true): Option[List[BNSInstance]] = {
    var attempt = 0
    var retryMillis = 0L
    var bnsList:Option[List[BNSInstance]] = None
    do {
      bnsList = getValidBNS(serviceName, valid)
      retryMillis = if(bnsList.isDefined) -1L else {
        attempt += 1
        val delayMillis = getDelayMillis(attempt)
        try {
          Thread.sleep(delayMillis)
        } catch {
          case e:Exception => logger.warn("Delay interrupted " + e.getMessage)
        }
        delayMillis
      }
    } while(retryMillis > 0L)
    bnsList
  }

  private def getValidBNS(serviceName: String, valid: Boolean,
                          timeout: Int = TIME_OUT_MILLIS) : Option[List[BNSInstance]] = {
    try {
      val bnsClient = new BNSClient
      val bnsList = bnsClient.getInstanceByService(serviceName, timeout)
      if (bnsList == null) {
        throw new Exception(s"Cannot get BNS [$serviceName] instance, return NULL")
      }
      val bnsInstances = synchronized {
        if(valid) {
          bnsList.asScala.filterNot(_.getStatus != 0).toList
        } else {
          bnsList.asScala.toList
        }
      }
      logger.debug("Get BNSInstance List")
      Some(bnsInstances)
    } catch {
      case e: Exception =>
        logger.warn("Encounter an error when using BNS:" + e.getMessage)
        None
    }
  }

  private def getDelayMillis(attempt: Int) : Long = if(attempt - 1 > 3) -1L else (1 << attempt) * 500L

  private def toBnsServer(bns: BNSInstance): BnsServer = {
    BnsServer(bns.getInstanceId, bns.getHostName, bns.getDottedIP, bns.getStatus + "",
      bns.getInterStatus+"", bns.getPort+"", bns.getInterStatus+"", bns.getOffset+"",
      bns.getTag, bns.getServiceName, bns.getExtra, bns.getMultiPort, "",
      "", "", "", "", "")
  }
}

case class BnsServer(id: String,
                     hostName: String,
                     vnetIp: String,
                     status: String,
                     interventionalStatusMeaning: String,
                     port: String,
                     interventionalStatus: String,
                     offset: String,
                     tag: String,
                     bnsName: String,
                     extra: String,
                     multi_port: String,
                     portInfo: String,
                     deployPath: String,
                     runUser: String,
                     healthCheckCmd: String,
                     healthCheckType: String,
                     containerId: String) extends Serializable

case class BnsMessage(success: Boolean, message: String, data: Option[List[BnsServer]]) extends Serializable

