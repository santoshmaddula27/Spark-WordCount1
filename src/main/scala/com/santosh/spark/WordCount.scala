package com.santosh.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("book")
    val words = input.flatMap(line => line.split(' '))
    val lowerCaseWords = words.map(word => word.toLowerCase())
    val wordCounts = lowerCaseWords.countByValue()

    val sample = wordCounts.take(20)

    for ((word, count) <- sample) {
      println(word + " " + count)
    }

    sc.stop()
  }
}
