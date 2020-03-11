package org.flame.streaming.policy

import org.flame.streaming.Logging
import org.flame.streaming.config.FlameConfig

/**
  * Created by cloud on 2020/03/03.
  */
trait Policy extends Logging {

  val flameConfig: FlameConfig

  def run(): Unit

}
