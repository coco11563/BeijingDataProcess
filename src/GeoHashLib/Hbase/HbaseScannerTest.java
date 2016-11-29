package GeoHashLib.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2016/11/28.
 *
 * 关键字检索类的MR形式
 */
public class HbaseScannerTest {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = new Job(conf, "KeyWords Counter");
        job.setJarByClass(HbaseScannerTest.class);
        Scan scan = new Scan();
        scan.addColumn(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL);
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toString(CheckinDAO.TABLE_NAME),
                scan,
                Mapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job,
                true //addDependencyJars upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
        );
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class Mapper extends TableMapper<Text, LongWritable> {
        static enum Counters{ROWS, KEYWORDS};
        private int containsKeywords(String msg) {
            String keyword = "李志";
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

    }
}
