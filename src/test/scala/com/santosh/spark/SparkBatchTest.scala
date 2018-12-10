package com.santosh.spark

class SparkBatchTest extends EnvironmentInitializerSC{

  "should test wordcount" in {
    val lines = Array("to be or not to be", "that is the question")
    val rdd = sc.parallelize(lines)
    val words = rdd.flatMap(line => line.split(' '))
    val lowerCaseWords = words.map(word => word.toLowerCase())
    val wordCounts = lowerCaseWords.countByValue().take(20)

    wordCounts should contain allOf  (("be", 2), ("is", 1), ("not", 1), ("or", 1), ("question", 1), ("that", 1), ("to", 2), ("the",1))
  }

  "should do wordcount on empty rdd" in {
    val lines = Array.empty[String]
    val rdd = sc.parallelize(lines)
    val wordCount = WordCountTest.count(rdd).collect()
    wordCount shouldBe empty
  }

}
