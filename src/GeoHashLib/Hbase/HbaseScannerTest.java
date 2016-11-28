package GeoHashLib.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.zookeeper.Op;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/28.
 */
public class HbaseScannerTest {
    private static String keyword = "李志";
    public static class Mapper extends TableMapper<Text, LongWritable> {
        public static enum Counters{ROWS, KEYWORDS};
        private int containsKeywords(String msg) {
            String[] ress  = msg.split(keyword);
            return ress.length;
        }
        @Override
        protected void map(ImmutableBytesWritable rowkey, Result result, Context context){
            byte[] b = result.getColumnLatestCell(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL).getValueArray();
            String msg = Bytes.toString(b);
            if (msg != null && !msg.isEmpty())
                context.getCounter(Counters.ROWS).increment(1);
            int keynum = containsKeywords(msg);
            if (keynum > 1) {
                context.getCounter(Counters.KEYWORDS).increment(keynum - 1);
            }
        }
        public static void main(String args) throws IOException {
            Configuration conf = HBaseConfiguration.create();
            Job job = new Job(conf, "KeyWords Counter");
            job.setJarByClass(HbaseScannerTest.class);
            Scan scan = new Scan();
        }
    }
}
