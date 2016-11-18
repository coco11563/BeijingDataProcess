package MysqlToHbase;

import dataRead.CheckIn;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static MysqlToHbase.HbaseQL.getTable;
import static MysqlToHbase.HbaseQL.put;

/**
 * Created by coco1 on 2016/11/18.
 * 对象化Hbase操作
 * 用这个实现一个线程从阻塞队列中不断提取@Checkin
 *
 * 然后解析并注入到Hbase中
 *
 * CheckInReadThread.CIRTBQ
 **/
public class HbaseOperation {
    private static Logger logger = Logger.getLogger(HbaseOperation.class);
    public static class InsertThread extends Thread {

        @Override
        public void run(){
            super.run();
            try {
                insertIntoHbase();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        void insertIntoHbase() throws IOException {
            int i = 0;
            ArrayList<Put> putDateList = new ArrayList<Put>();
            HTable cityTable = getTable();
            cityTable.setAutoFlushTo(false);//
            while(true) {
                if (i > 100) {
                    cityTable.put(putDateList);
                    cityTable.flushCommits();
                    putDateList.clear();
                    logger.info("进行一次写入");
                    this.interrupt();
                }
                try {
                    i = 0;
                    CheckIn c = cirBQ.take();
                    Put p = put(c);
                    putDateList.add(p);
                    if (putDateList.size() > 1000){
                        cityTable.put(putDateList);
                        cityTable.flushCommits();
                        putDateList.clear();
                        logger.info("进行一次写入");
                    }
                } catch (InterruptedException e) {
                    i ++;
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }


}
