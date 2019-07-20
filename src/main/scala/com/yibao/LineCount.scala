package com.yibao

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author liuwenyi
  * @date 2019/07/19
  */
object LineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/line_count.txt")
    val pairs = lines.map(line => (line, 1))
    val lineCount = pairs.reduceByKey(_ + _)
    lineCount.foreach(lineCount => println(lineCount._1, lineCount._2))

  }
}
