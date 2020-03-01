package com.wdl.flink.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {

  def getRedisSink(): RedisSink[(String, Int)] ={
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    val redisSink: RedisSink[(String, Int)] = new RedisSink[(String, Int)](config, new MyRedisMapper)
    redisSink
  }

  class MyRedisMapper extends RedisMapper[(String, Int)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "redisSink")
    }

    override def getKeyFromData(t: (String, Int)): String = t._1

    override def getValueFromData(t: (String, Int)): String = t._2.toString
  }
}
