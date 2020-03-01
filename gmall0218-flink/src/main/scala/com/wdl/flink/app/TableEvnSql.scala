package com.wdl.flink.app

import com.alibaba.fastjson.JSON
import com.wdl.flink.bean.StartUpLog
import com.wdl.flink.util.MyKafkaUtil
import com.wdl.gmall0218.common.GmallConstant
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}

object TableEvnSql {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val fkc: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer(GmallConstant.KAFKA_TOPIC_STARTUP)

    val value: DataStream[String] = environment.addSource(fkc)

    val kafkaDS: DataStream[StartUpLog] = value.map { json => JSON.parseObject(json, classOf[StartUpLog])}

    val value1: DataStream[StartUpLog] = kafkaDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.milliseconds(0L)) {
        override def extractTimestamp(element: StartUpLog) = {
          element.ts
        }
      }
    ).setParallelism(1)

    val te: StreamTableEnvironment = TableEnvironment.getTableEnvironment(environment)

    //把数据流转化成Table
    val startupTable: Table = te.fromDataStream(value1 , 'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour, 'ts.rowtime)
//    val table: Table = te.fromDataStream(value1)

    //通过table api 进行操作
    // 每10秒 统计一次各个渠道的个数 table api 解决
    //1 groupby  2 要用 window   3 用eventtime来确定开窗时间

    val table: Table = startupTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch, 'tt).select('ch, 'ch.count)

    val value2: DataStream[(Boolean, (String, Long))] = table.toRetractStream[(String, Long)]


    value2.filter(_._1).print()

    environment.execute()

  }
}
