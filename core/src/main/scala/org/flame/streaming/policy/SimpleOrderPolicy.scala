package org.flame.streaming.policy

import org.flame.streaming.config.FlameConfig

/**
  * Created by cloud on 2020/03/03.
  */
class SimpleOrderPolicy(val flameConfig: FlameConfig) extends Policy {

  override def run(): Unit = {
    val tables = flameConfig.getTableConfig.getTables
    tables.foreach(_.createTable())
  }

}
