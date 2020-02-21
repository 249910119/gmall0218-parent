package com.wdl.gmall0218.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wdl.gmall0218.common.GmallConstant;
import com.wdl.gmall0218.utils.MyKafkaSender;

import java.util.List;
import java.util.Random;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDatasList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDatasList;
    }

    public void handle() {

        if ("order_info".equals(this.tableName) && CanalEntry.EventType.INSERT == this.eventType){
            rowDateListToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        } else if ("user_info".equals(this.tableName) && ((CanalEntry.EventType.INSERT == this.eventType) || (CanalEntry.EventType.UPDATE == this.eventType))) {
            rowDateListToKafka(GmallConstant.KAFKA_TOPIC_USER_INFO);
        } else if ("order_detail".equals(this.tableName) && CanalEntry.EventType.INSERT == this.eventType){
            rowDateListToKafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
        }
    }

    /**
     * 统一处理，发送到 kafka
     * @param topic
     */
    private void rowDateListToKafka(String topic){

        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.print(column.getName() + " : " + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            MyKafkaSender.send(topic, jsonObject.toJSONString());
            try {
                Thread.sleep(new Random().nextInt(5)* 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
