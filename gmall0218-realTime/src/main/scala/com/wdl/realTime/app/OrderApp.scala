package com.wdl.realTime.app

import com.alibaba.fastjson.JSON
import com.wdl.gmall0218.common.GmallConstant
import com.wdl.realTime.bean.OrderInfo
import com.wdl.realTime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * 将订单数据写入 Hbase 中
 */
object OrderApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val orderInputDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_NEW_ORDER, ssc)

    val orderDS: DStream[OrderInfo] = orderInputDS.map(ordr => {
      val jsonString: String = ordr.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      // TODO 电话脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"
      // TODO 补充时间
      val times: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = times(0)
      orderInfo.create_hour = times(1).split(":")(0)
      orderInfo
    })

    orderDS.foreachRDD(orderRDDs => {
      orderRDDs.saveToPhoenix("GMALL0218_ORDER_INFO",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
