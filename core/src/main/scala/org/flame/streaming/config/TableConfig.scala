package org.flame.streaming.config

import org.flame.streaming.env.SQLEnv
import org.flame.streaming.metadata.MetaDataClient

/**
  * Created by cloud on 2020/03/03.
  */
/* 这个类目前来看可能有点冗余 */
class TableConfig(flameConfig: FlameConfig) extends Serializable {
  private val prefix: String = "flame"

  private val tables: Array[Table] = parseTable()

  private def parseTable(): Array[Table] = {
    val tp = s"$prefix.table"
    flameConfig.get(s"$prefix.tables").split(",").map(k => Table.parseTable(flameConfig, tp, k))
  }

  def getTables: Array[Table] = tables

}

sealed trait Table extends Serializable {
  val name: String
  val sql: String

  def createTable(): Unit
}

object Table {

  def parseTable(flameConfig: FlameConfig, prefix: String, name: String): Table = {
    val tableType = flameConfig.get(s"$prefix.$name.type")
    tableType.toLowerCase match {
      case "factory" => FactoryTable(prefix, name, flameConfig)
      case "query" => QueryTable(prefix, name, flameConfig)
      case _ => throw new UnsupportedOperationException(s"Unknown options $tableType")
    }
  }

}

/**
  * 通过DDL创建的表
  * @param name             表名
  * @param sql              创建表的sql语句
  * @param isRemote         是否使用远程模式, 若使用则通过元数据平台请求表结构
  * @param metadataUrl      元数据平台地址, 远程模式时使用
  * @param database         库名, 远程模式时使用
  * @param customParameters 构建DDL使用的参数列表,远程模式时使用
  */
case class FactoryTable(name: String,
                        sql: String,
                        isRemote: Boolean,
                        metadataUrl: String,
                        database: String,
                        customParameters: Map[String, String]) extends Table {

  override def createTable(): Unit = {
    val tableEnv = SQLEnv.getTableEnv
    tableEnv.sqlUpdate(getDDL)
  }

  /**
    * 返回当前可用的ddl语句, 若用户设置了远程模式则通过元数据平台合成ddl语句
    * 若未合成ddl语句则检查sql语句是否设置, 未设置则抛出异常
    */
  def getDDL: String = {
    if (isRemote) {
      val ddl = fetchMetadataDDL
      if (ddl != "") ddl else if (sql != "") sql else throw new NoSuchElementException("ddl can not be empty")
    } else {
      if (sql != "") sql else throw new NoSuchElementException("sql can not be empty")
    }
  }

  /**
    * 通过元数据平台来构建创建表所需要的ddl语句
    * @return  返回一个符合sql语法的创建表所使用的ddl语句
    */
  def fetchMetadataDDL: String = {
    val client = MetaDataClient(metadataUrl)
    val md = client.fetchMetadata(database, name)
    val withParams = customParameters.map { case (k, v) => s"'$k'='$v'" }.mkString("(",",",")")
    s"${md.toSqlString} with $withParams"
  }
}

object FactoryTable {

  def apply(prefix: String, name: String, flameConfig: FlameConfig): FactoryTable = {
    val params = Array("sql","remote","type")
    val tablePrefix = s"$prefix.$name"
    val remote = flameConfig.getBoolean(s"$tablePrefix.remote", false)
    val db = flameConfig.get(s"$tablePrefix.database","")
    val sql = flameConfig.get(s"$tablePrefix.sql", "")
    val url = if (remote) flameConfig.getMetadataUrl else ""
    val custom = flameConfig.getAllWithPrefix(s"$tablePrefix.").filter { case (k, _) => ! params.contains(k) }

    FactoryTable(name, sql, remote, url, db, custom.toMap)
  }

}

case class QueryTable(name: String,
                      sql: String) extends Table {

  override def createTable(): Unit = {
    val _sql = if(sql != null && sql != "") sql else throw new NoSuchElementException("sql can not be empty")
    val tableEnv = SQLEnv.getTableEnv
    val res = tableEnv.sqlQuery(_sql)
    tableEnv.registerTable(name, res)
  }

}

object QueryTable {
  def apply(prefix: String, name: String, flameConfig: FlameConfig): QueryTable = {
    val sql = flameConfig.get(s"$prefix.$name.sql")
    QueryTable(name, sql)
  }
}
