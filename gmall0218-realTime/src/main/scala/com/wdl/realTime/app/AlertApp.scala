package com.wdl.realTime.app

import java.util

import com.alibaba.fastjson.JSON
import com.wdl.gmall0218.common.GmallConstant
import com.wdl.realTime.bean.{CouponAlertInfo, EventInfo}
import com.wdl.realTime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alert_app")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaInputDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)

    val eventInfoDS: DStream[EventInfo] = kafkaInputDS.map(record => {
      val value: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(value, classOf[EventInfo])
      eventInfo
    })

    val eventByMidDS: DStream[(String, Iterable[EventInfo])] = eventInfoDS.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

    val eventWinDS: DStream[(String, Iterable[EventInfo])] = eventByMidDS.window(Seconds(30), Seconds(2))

    val eventInfoFlagDS: DStream[(Boolean, CouponAlertInfo)] = eventWinDS.map {
      case (mid, eventInfos) => {
        //用户id集合
        val uids = new util.HashSet[String]()
        //用户领券的商品id
        val itemIds = new util.HashSet[String]()
        //用户行为
        val events = new util.ArrayList[String]()
        //是否浏览了商品
        var clickItemFlag = false
        breakable(
          for (eventInfo <- eventInfos) {
            events.add(eventInfo.evid)
            if (eventInfo.evid == "coupon") {
              uids.add(eventInfo.uid)
              itemIds.add(eventInfo.itemid)
            } else if (eventInfo.evid == "clickItem") {
              clickItemFlag = true
              break()
            }
          }
        )
        val couponAlertInfo: CouponAlertInfo = CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
        val flag = (uids.size() >= 3 && !clickItemFlag)
        (flag, couponAlertInfo)
      }
    }

    val couponAlertInfoDS: DStream[CouponAlertInfo] = eventInfoFlagDS.filter(_._1).map(_._2)

    couponAlertInfoDS.foreachRDD(rdd =>{
      rdd.foreachPartition(cai => {
        val list: List[(String, CouponAlertInfo)] = cai.toList.map(ca => (ca.mid + "_" + (ca.ts / 1000 / 60), ca))
        println(list.mkString("\n"))
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_NEW_ORDER, list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
