package generateFakeData;

import MysqlToHbase.HbaseQL;
import dataRead.CheckIn;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

import static MysqlToHbase.HbaseQL.getTable;
import static MysqlToHbase.HbaseQL.put;
import static MysqlToHbase.HbaseQL.rsBQ;

/**
 * Created by coco1 on 2016/11/21.
 *
 */
public class HBaseGenerater {
    private static Logger logger = Logger.getLogger(HBaseGenerater.class);
    public static void main(String args[]) {
        scannerThread scannerThread = new scannerThread();
        scannerThread.start();
        for (int i = 0 ; i < 4 ; i++){
            insertionThread insertionThread = new insertionThread();
            insertionThread.start();
        }
        while(true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(rsBQ.size());
        }
 }
 private static class insertionThread extends Thread{

        @Override
     public void run(){
            try {
                Insetion();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public void Insetion() throws IOException {
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
                    CheckIn c = rsBQ.take();
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


    private static class scannerThread extends Thread{
        @Override
        public void run(){
            try {
                HbaseQL.scan("checkinInform");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
