package GeoHashLib.Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


/**
 * Created by Administrator on 2016/11/23.
 *
 */
public class HbaseNewTest {
    public static void main(String args[]) {
        try {
            Configuration cfg = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(cfg);
//            Admin admin  = conn.getAdmin();
            TableName tableName = TableName.valueOf("user");
            Table table = conn.getTable(tableName);
//            HTableMultiplexer hTableMultiplexer = new HTableMultiplexer(cfg,1);
            Put p = new Put(Bytes.toBytes("TheRealMT1"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("JSJ"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("email"),
                    Bytes.toBytes("JSJ@163.com"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("password"),
                    Bytes.toBytes("XM123"));
//            if (hTableMultiplexer.put(tableName, p))
//                System.out.println("-----------yeah!----------");
            table.put(p);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
