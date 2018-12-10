
package com.santosh.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

trait EnvironmentInitializerSC extends WordSpec with Matchers with BeforeAndAfterAll{
  private val master = "local[*]"
  private val appName = "WordCount"

  var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
  }

}