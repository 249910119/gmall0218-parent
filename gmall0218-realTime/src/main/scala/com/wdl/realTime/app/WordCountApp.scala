package com.wdl.realTime.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]): Unit = {

    val wordCount: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

    val ssc = new SparkContext(wordCount)

    val wordRDD: RDD[(String, Int)] = ssc.makeRDD(Array(("a", 1), ("a", 3), ("b", 4), ("b", 6), ("c", 5)))

    val result: RDD[(String, Int)] = wordRDD.groupByKey().map { case (k, v) => (k, v.sum / v.size) }

    result.collect.foreach(println)

  }
}
