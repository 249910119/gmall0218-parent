package com.wdl.flink.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MyJdbcSink(sql: String) extends RichSinkFunction[Array[Any]]{

  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://localhost:3306/dobo?useSSL=false"

  val username="root"

  val password="root"

  val maxActive="20"

  var connection:Connection=null;

  // TODO 打开连接池
  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)

//    connection = DruidDataSourceFactory.createDataSource(properties).getConnection
    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()

  }

  // TODO 执行业务逻辑
  override def invoke(value: Array[Any]): Unit = {
    val statement: PreparedStatement = connection.prepareStatement(sql)

    for( i <- 0 to (value.length - 1) ){
      statement.setObject(i + 1 , value(i))
      println("保存：" + value(i))
    }
    statement.execute()
  }

  // TODO 关闭连接池
  override def close(): Unit = {
    if (connection != null){
      connection.close()
    }
  }
}
