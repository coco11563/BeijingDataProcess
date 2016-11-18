package MysqlToHbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by coco1 on 2016/11/17.
 */
public class ExampleClient {
    private static Configuration cfg = HBaseConfiguration.create();
    public static void main(String args[]) {
        try {
            HBaseAdmin admin = new HBaseAdmin(cfg);
            HTableDescriptor htd = new HTableDescriptor("test");
            HColumnDescriptor hcd = new HColumnDescriptor("data");
            htd.addFamily(hcd);
            admin.createTable(htd);
            HTableDescriptor[] htds = list(admin);
            for (HTableDescriptor htdd : htds) {
                System.out.println(htdd.getNameAsString());
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HTableDescriptor[] list(HBaseAdmin admin) throws IOException {
        return admin.listTables();
    }
}
