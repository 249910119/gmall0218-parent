package com.wdl.realTime.app

import java.util

import com.alibaba.fastjson.JSON
import com.wdl.gmall0218.common.GmallConstant
import com.wdl.realTime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.wdl.realTime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoInDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailInDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    // TODO 处理订单
    val orderInfoDS: DStream[OrderInfo] = orderInfoInDS.map(record => {
      val orderInfoJSON: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(orderInfoJSON, classOf[OrderInfo])
      // TODO 补充时间字段
      val dateArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateArr(0)
      orderInfo.create_hour = dateArr(1).split(":")(0)
      orderInfo
    })

    // TODO 处理订单详情
    val orderDetailDS: DStream[OrderDetail] = orderDetailInDS.map(record => {
      val orderDetailJSON: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(orderDetailJSON, classOf[OrderDetail])
      orderDetail
    })

    // TODO 双流Join之前应该转换为 (k, v) 格式
    val orderInfoMapDS: DStream[(String, OrderInfo)] = orderInfoDS.map(oi => (oi.id, oi))
    val orderDetailMapDS: DStream[(String, OrderDetail)] = orderDetailDS.map(od => (od.order_id, od))

    // 为了不管是否能够关联左右 ，都要保留左右两边的数据 采用full join
    val fullInfoDS: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoMapDS.fullOuterJoin(orderDetailMapDS)

    val saleDetailDS: DStream[SaleDetail] = fullInfoDS.mapPartitions(fullInfo => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      implicit val formats = org.json4s.DefaultFormats
      val saleDetails: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

      for ((orderId, (orderInfoOption, orderDetailOption)) <- fullInfo) {

        // TODO 主表有数据
        if (orderInfoOption != None) {
          val orderInfo: OrderInfo = orderInfoOption.get
          // TODO 从表有数据，直接关联
          if (orderDetailOption != None) {
            println(" 主表有数据 ！且从表有数据 成功关联")
            val orderDetail: OrderDetail = orderDetailOption.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetails.+=(saleDetail)
          }
          // TODO 将主表数据写入到缓存中
          val orderInfoRedisKey = "order_info:" + orderId
          /* fastjson无法转换 case class 为json 例：val orderInfoJson: String = JSON.toJSONString(orderInfo) */
          val ordJson: String = Serialization.write(orderInfo)
          jedisClient.setex(orderInfoRedisKey, 300, ordJson)

          // TODO 从缓存中查询从表数据
          val orderDetailRedisKey = "order_detail:" + orderId
          val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailRedisKey)
          import collection.JavaConversions._
          if (orderDetailSet.size() > 0) {
            for (orderDetailJson <- orderDetailSet) {
              println(" 查询到从表缓存数据进行关联")
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              saleDetails.+=(saleDetail)
            }
          }
        } else if (orderDetailOption != None) {
          // TODO 主表没有数据
          val orderDetail: OrderDetail = orderDetailOption.get
          val ordRedisKey = "order_info:" + orderDetail.order_id
          val ordJSON: String = jedisClient.get(ordRedisKey)
          if (ordJSON != None) {
            val orderInfo: OrderInfo = JSON.parseObject(ordJSON, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetails.+=(saleDetail)
          }

          val orderDetailRedisKey = "order_detail:" + orderId
          val orddJSONString: String = Serialization.write(orderDetail)
          jedisClient.sadd(orderDetailRedisKey, orddJSONString)
          jedisClient.expire(orderDetailRedisKey, 300)
        }
      }
      jedisClient.close()
      saleDetails.toIterator
    })

    saleDetailDS.foreachRDD(rdd => {
      println(rdd.collect().mkString("\n"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
