package com.wdl.flink.util

import java.util
import java.util.List

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wdl.flink.bean.StartUpLog
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object MyEsUtil {

  val  httpHosts: util.List[HttpHost] = new util.ArrayList[HttpHost]();
  httpHosts.add(new HttpHost("hadoop102", 9200))
  httpHosts.add(new HttpHost("hadoop103", 9200))
  httpHosts.add(new HttpHost("hadoop104", 9200))

  def getESSink(indexName: String): ElasticsearchSink[String] ={

    val esFunc: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
      override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        println("试图保存：" + element)
        val jsonObj: JSONObject = JSON.parseObject(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonObj)
        requestIndexer.add(indexRequest)
      }
    }

    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, esFunc)

    sinkBuilder.setBulkFlushMaxActions(10)
    sinkBuilder.build()
  }
}
