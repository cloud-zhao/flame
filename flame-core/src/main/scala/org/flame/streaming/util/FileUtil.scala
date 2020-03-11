package org.flame.streaming.util

import java.io.{File, FileWriter}

/**
  * Created by cloud on 2020/02/21.
  */
object FileUtil {
  def readFile(filePath: String) = {
    val file = scala.io.Source.fromFile(filePath)
    try {
      file.getLines().mkString
    } finally {
      file.close()
    }
  }

  def createTempFile(content: String, prefix: String, suffix: String) = {
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.deleteOnExit()
    val wrt = new FileWriter(tempFile)
    wrt.write(content)
    wrt.close()

    tempFile.getAbsolutePath
  }
}
