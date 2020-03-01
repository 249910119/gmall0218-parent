package com.wdl.flink.app

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

object TimeWindows3 {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sockDS: DataStream[String] = environment.socketTextStream("hadoop102", 7777)

    val sockMapDS: DataStream[(String, Long, Int)] = sockDS.map(sock => {
      val arr: Array[String] = sock.split(" ")
      (arr(0), arr(1).toLong, 1)
    })

    val waterDS: DataStream[(String, Long, Int)] = sockMapDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(3000L)) {
        override def extractTimestamp(t: (String, Long, Int)) = {
          t._2
        }
      }
    ).setParallelism(1)

    val waterTuple: KeyedStream[(String, Long, Int), Tuple] = waterDS.keyBy(0)
    waterTuple.print("Key : ")

    val winDS: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = waterTuple.window(
      EventTimeSessionWindows.withGap(Time.milliseconds(4000L)))

    val value: DataStream[(String, Long, Int)] = winDS.reduce((w1, w2) => {
      (w1._1 + "->" + w1._2 + " : " + w2._2, w1._2 + w2._2, w1._3 + w1._3)
    })

    val resDS: DataStream[(String, Long, Int)] = winDS.sum(2)

    value.print("result : ")
    environment.execute()

  }
}
