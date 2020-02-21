package com.wdl.gmall0218.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wdl.gmall0218.handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example1",
                "",
                "");
        while (true){
            canalConnector.connect();
            // TODO 抓取 gmall0218 中的 order_info 表
            canalConnector.subscribe("gmall0218.*");

            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> messageEntries = message.getEntries();

            int size = messageEntries.size();

            if (size == 0){
                System.out.println("暂时没有数据！");
                try {
                    Thread.sleep(2000
                    );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 每个Entry对应一个sql语句
                for (CanalEntry.Entry messageEntry : messageEntries) {
                    // TODO 过滤 entry 因为不是每个sql都是对数据库进行修改的写操作，例：开关事务
                    if (messageEntry.getEntryType() == CanalEntry.EntryType.ROWDATA){

                        try {
                            // TODO 将Entry中的StoreValue进行反序列化得到结构化的对象
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(messageEntry.getStoreValue());
                            // TODO 获取表名
                            String tableName = messageEntry.getHeader().getTableName();
                            // TODO 获取SQL语句的类型，例：insert update delete？
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            // TODO 数据行集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                            canalHandler.handle() ;
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
