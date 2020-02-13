package com.wdl.realTime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wdl.gmall0218.common.GmallConstant
import com.wdl.realTime.bean.StartUpLog
import com.wdl.realTime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("gmall0218").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Seconds(5))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    // TODO 拆分时间戳, 放到 logDate, logHour 中
    val startUpLogDS: DStream[StartUpLog] = kafkaDS.map(_.value()).map(log => {
      val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      val date = new Date(startUpLog.ts)
      val dateFormat: String = new SimpleDateFormat("yyyy-MM-dd HH").format(date)
      val dateArr: Array[String] = dateFormat.split(" ")

      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)

      startUpLog
    })

    startUpLogDS.cache()

    // TODO 通过历史数据去重
    val filterDS: DStream[StartUpLog] = startUpLogDS.transform(startUpLogRDD => {
      val jedisRead: Jedis = RedisUtil.getJedisClient
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val smembers: util.Set[String] = jedisRead.smembers("dau:" + dateStr)
      val broadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(smembers)
      jedisRead.close()
      startUpLogRDD.filter(startUpLog => {
        !broadcast.value.contains(startUpLog.mid)
      })
    })

    // TODO 批次内去重
    val flagMapDS: DStream[StartUpLog] = filterDS.map(sul => (sul.mid, sul)).groupByKey().map(sulMap => sulMap._2.take(1)).flatMap(sul => sul)

    // TODO 保存到 redis 中

    flagMapDS.foreachRDD(rdds => {
      rdds.foreachPartition(rdd => {
        val jedisWrite: Jedis = RedisUtil.getJedisClient
        rdd.foreach(sul => {
          jedisWrite.sadd("dau:" + sul.logDate, sul.mid)
        })
        jedisWrite.close()
      })
    })

    // TODO 保存在 HBase 中
    flagMapDS.foreachRDD(rdds => {
      rdds.saveToPhoenix("gmall0218_dau", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
