package MysqlToHbase;

import MRCompetion.ThreadClass.CheckInReadThread;

import static MRCompetion.Competion.printThreadList;
import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static MysqlToHbase.HbaseQL.columnFamily;
import static MysqlToHbase.HbaseQL.tableName;

/**
 * Created by coco1 on 2016/11/18.
 *
 * mysql to Hbase的主线程
 */
public class Main {
    public static void main(String args[]) throws InterruptedException {

        try {
            HbaseQL.create(tableName, columnFamily);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //1个mysqlGetter
        System.out.println("正在进行读取线程初始化...");
        CheckInReadThread c = new CheckInReadThread();
        c.start();
        //6个HBase Insertion
        System.out.println("正在进行更新线程初始化...");
        for (int i = 0 ; i < 6 ; i ++) {
            HbaseOperation.InsertThread insertThread = new HbaseOperation.InsertThread();
            insertThread.start();
        }while (true) {
            Thread.sleep(10000);
            System.out.println("当前队列内待处理的数据量为："+cirBQ.size());
            printThreadList();
        }
    }
}
