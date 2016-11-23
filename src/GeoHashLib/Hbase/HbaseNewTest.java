package GeoHashLib.Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by Administrator on 2016/11/23.
 */
public class HbaseNewTest {
    public static void main(String args[]) {
        try {
            Configuration cfg = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(cfg);
            Admin admin  = conn.getAdmin();
            TableName tableName = TableName.valueOf("user");
            HTableMultiplexer hTableMultiplexer = new HTableMultiplexer(cfg,1);
            Put p = new Put(Bytes.toBytes("TheRealMT"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("FanFan"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("email"),
                    Bytes.toBytes("FanFan@163.com"));
            p.addColumn(Bytes.toBytes("info"),
                    Bytes.toBytes("password"),
                    Bytes.toBytes("FanFan123"));
            hTableMultiplexer.put(tableName, p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
