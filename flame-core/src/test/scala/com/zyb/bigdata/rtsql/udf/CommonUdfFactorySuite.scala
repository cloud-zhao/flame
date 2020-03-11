package com.zyb.bigdata.rtsql.udf

import org.scalatest.FunSuite

/**
 * Author: zhangying14
 * Date: 2020/3/6 11:49
 * Package: com.zyb.bigdata.rtsql.udf
 * Description:
 *
 */
class CommonUdfFactorySuite extends FunSuite {
  test("JsonExtract") {
    val f = new JsonExtractFunction()
    val json =
      """
        |{"name": "Jeff Dean", "address": {"country": "AM", "company": "G"}}
        |""".stripMargin
    assert(f.eval(json, "$.name") == "Jeff Dean")
    assert(f.eval(json, "$.address.country") == "AM")
    assert(f.eval(json, "$.address.company") == "G")
  }
}
