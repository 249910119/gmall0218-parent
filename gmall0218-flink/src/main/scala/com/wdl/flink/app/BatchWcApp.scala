package com.wdl.flink.app

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object BatchWcApp {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val textDSet: DataSet[String] = env.readTextFile("/applog/flink/input.txt")

    import org.apache.flink.api.scala._

    val resultDSet: AggregateDataSet[(String, Int)] = textDSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    resultDSet.print()
    resultDSet.writeAsText("/applog/flink/output.txt")

    env.execute()
  }
}
