package GeoHashLib.Hbase;
import MRCompetion.ThreadClass.CheckInReadThread;
import dataRead.CheckIn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;


/**
 * Created by Administrator on 2016/11/23.
 * 一个比较靠谱的Mysql to HBase
 */
public class HbaseNewTest {
    public static void main(String args[]) throws IOException, InterruptedException {
        CheckinDAO cd = new CheckinDAO();
        CheckInReadThread cir = new CheckInReadThread();
        CheckIn c;
        cir.start();
        int NullGetTimes = 0;
        int n = 0;
        while(true) {
            if (NullGetTimes > 1000) {
                break;
            }
            try {
                NullGetTimes = 0;
                c = cirBQ.take();
                cd.addCheckin(c);
                n ++;
            } catch (InterruptedException e) {
                NullGetTimes++;
                Thread.sleep(1000);
                e.printStackTrace();
            }
        }
        System.out.println("seems all done");
        System.out.println("Insert " + n + "Checkin Data");
    }

public boolean put(Put put, Configuration cfg, Connection conn, TableName tableName) {
    try {
        Table table = conn.getTable(tableName);
        table.put(put);
        return true;
    } catch (IOException e) {
        e.printStackTrace();
        return false;
    }
}


}
