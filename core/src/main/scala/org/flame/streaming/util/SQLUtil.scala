package org.flame.streaming.util

/**
  * Created by cloud on 2020/02/21.
  */
object SQLUtil {
  def isValid(sql: String): Boolean = {
    isInsert(sql) || isSelect(sql)
  }

  def isSelect(sql: String): Boolean = {
    sql.toUpperCase().startsWith("SELECT")
  }

  def isInsert(sql: String): Boolean = {
    sql.toUpperCase().startsWith("INSERT INTO")
  }
}
