package com.wdl.flink.app

import com.alibaba.fastjson.JSON
import com.wdl.flink.bean.StartUpLog
import com.wdl.flink.util.MyKafkaUtil
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}

object TableEvnSql2 {

  //每10秒中渠道为appstore的个数
  def main(args: Array[String]): Unit = {
    //sparkcontext
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //时间特性改为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从kafka中获取流
    val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val startupLogDstream: DataStream[StartUpLog] = dstream.map(jsonString =>JSON.parseObject(jsonString,classOf[StartUpLog]))
    //告知watermark 和 eventTime如何提取
    val startupLogWithEventTimeDStream: DataStream[StartUpLog] = startupLogDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.seconds(0L)) {
      override def extractTimestamp(element: StartUpLog): Long = {
        element.ts
      }
    }).setParallelism(1)

    //SparkSession
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //把数据流转化成Table
    val startupTable: Table = tableEnv.fromDataStream(startupLogWithEventTimeDStream , 'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'ts.rowtime)

    //通过table api 进行操作
    // 每10秒 统计一次各个渠道的个数 table api 解决
    //1 groupby  2 要用 window   3 用eventtime来确定开窗时间
    val resultTable: Table = startupTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch,'tt ).select( 'ch, 'ch.count)



    //把Table转化成数据流
    //val appstoreDStream: DataStream[(String, String, Long)] = appstoreTable.toAppendStream[(String,String,Long)]
    val resultDstream: DataStream[(Boolean, (String, Long))] = resultTable.toRetractStream[(String,Long)]

    resultDstream.filter(_._1).print()

    env.execute()

  }



}
