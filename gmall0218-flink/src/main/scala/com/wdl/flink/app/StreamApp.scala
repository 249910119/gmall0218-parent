package com.wdl.flink.app

import com.alibaba.fastjson.JSON
import com.wdl.flink.bean.StartUpLog
import com.wdl.flink.util.{MyEsUtil, MyJdbcSink, MyKafkaUtil, MyRedisUtil}
import com.wdl.gmall0218.common.GmallConstant
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

object StreamApp {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val fkc: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer(GmallConstant.KAFKA_TOPIC_STARTUP)

    val value: DataStream[String] = environment.addSource(fkc)

    val value1: DataStream[StartUpLog] = value.map { json => JSON.parseObject(json, classOf[StartUpLog])}



    //val value2: DataStream[String] = value1.map(_.toString)
    //val value2: DataStream[(String, Int)] = value1.map(su => (su.ch, 1)).keyBy(0).sum(1)

    //sink 连接 redis
    //value2.addSink(MyRedisUtil.getRedisSink())

    // TODO sink 连接 ES
    //val esSink: ElasticsearchSink[String] = MyEsUtil.getESSink("gmall0218_es_sink")

    // TODO sink 连接 MySQL
    //val jdbcSink = new MyJdbcSink("insert into gmall.gmall_mysql_sink values(?,?,?)")
    //value1.map(v => Array[Any](v.uid, v.mid, v.appid)).addSink(jdbcSink)

    environment.execute()

  }

}
