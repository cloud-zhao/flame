package org.flame.streaming

import org.flame.streaming.config.FlameConfig
import org.flame.streaming.env.SQLEnv
import org.flame.streaming.policy.{Policy, SimpleOrderPolicy}
import org.flame.streaming.udf.UdfFactory

/**
  * Created by cloud on 2020/03/03.
  */
object LauncherMain extends Logging {

  def main(args: Array[String]): Unit = {

    var propertiesFile: String = ""
    var argv = args.toList

    while (argv.nonEmpty) {
      argv match {
        case "--properties-file" :: value :: tail =>
          propertiesFile = value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }

    val flameConfig = FlameConfig.parseFile(propertiesFile)
    val policy = getPolicy(flameConfig)

    registerUdf(flameConfig)
    policy.run()

    SQLEnv.execute(flameConfig.getJobName)

  }

  /**
    * 通过反射的方式来加载策略类, 默认策略类为{@link SimpleOrderPolicy}
    * @param flameConfig  通用配置
    * @return 返回策略类
    */
  private def getPolicy(flameConfig: FlameConfig): Policy = {
    val className = flameConfig.getPolicy
    val supervisor = classOf[Policy]
    val clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader)
    if (supervisor.isAssignableFrom(clazz)) {
      val constructor  = clazz.getConstructor(classOf[FlameConfig])
      constructor.newInstance(flameConfig).asInstanceOf[Policy]
    } else throw new Exception("Must be a subclass of Policy")
  }

  private def registerUdf(flameConfig: FlameConfig): Unit = {
    val udfs = flameConfig.getUdfList
    val supervisor = classOf[UdfFactory]
    udfs.foreach(className => {
      val clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader)
      if (supervisor.isAssignableFrom(clazz)) {
        val constructor  = clazz.getConstructor(classOf[FlameConfig])
        val udf = constructor.newInstance(flameConfig).asInstanceOf[UdfFactory]
        udf.register()
      } else throw new Exception("Must be a subclass of UdfFactory")
    })
  }

  private def printUsageAndExit(): Unit = {
    println(
      """
        |Usage LauncherMain [option]
        |
        |Options are:
        |   --properties-file  <配置文件>
      """.stripMargin)
    System.exit(1)
  }

}
