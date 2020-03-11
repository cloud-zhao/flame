package org.flame.streaming.util

import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row

/**
  * Created by cloud on 2020/02/27.
  */
object SchemaUtil {

  def row2map(row: Row, schema: TableSchema): Map[String, AnyRef] = {
    Map.empty
  }

}
