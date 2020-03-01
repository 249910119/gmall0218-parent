package com.wdl.flink.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WcStream {

  def main(args: Array[String]): Unit = {
    //从外部命令中获取参数
//    val tool: ParameterTool = ParameterTool.fromArgs(args)
//    val host: String = tool.get("host")
//    val port: Int = tool.get("port").toInt

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream("hadoop102",7777)
    // flatMap和Map需要引用的隐式转换
    import org.apache.flink.api.scala._
    //处理 分组并且sum聚合
    val dStream: DataStream[(String, Int)] = textDstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    //打印
    dStream.print()

    env.execute()
  }



}
