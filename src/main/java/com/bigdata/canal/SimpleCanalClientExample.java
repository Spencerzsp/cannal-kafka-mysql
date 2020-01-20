package com.bigdata.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleCanalClientExample {

    public static void main(String[] args) {

        //创建连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("wbbigdata01", 11111),
                "example", "root", "bigdata");

        int batchSize = 10000;
        int emptyCount = 0;

        try {
          connector.connect();
          connector.subscribe(".*\\..*");
          connector.rollback();
          int totalEmptyCount = 120;

          while (emptyCount < totalEmptyCount) {
              Message message = connector.getWithoutAck(batchSize);
              long batchId = message.getId();
              int size = message.getEntries().size();

              if (batchId == -1 || size == 0) {
                  emptyCount++;
                  System.out.println("empty count: " + emptyCount);
                  try{
                      Thread.sleep(1000);
                  } catch (InterruptedException e){

                  }
              }else {
                  emptyCount = 0;
//                  System.out.println(message.getEntries());
                  printEntry(message.getEntries());
              }
              connector.ack(batchId);
          }
            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entries) {

        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND){
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {

        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());

        }
    }
}
