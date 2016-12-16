package GeoHashLib.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2016/11/28.
 *
 * 关键字检索类的MR形式
 *
 * Counters: 32
 File System Counters
 FILE: Number of bytes read=0
 FILE: Number of bytes written=137982
 FILE: Number of read operations=0
 FILE: Number of large read operations=0
 FILE: Number of write operations=0
 HDFS: Number of bytes read=75
 HDFS: Number of bytes written=0
 HDFS: Number of read operations=1
 HDFS: Number of large read operations=0
 HDFS: Number of write operations=0
 Job Counters
 Launched map tasks=1
 Rack-local map tasks=1
 Total time spent by all maps in occupied slots (ms)=287458
 Total time spent by all reduces in occupied slots (ms)=0
 Total time spent by all map tasks (ms)=143729
 Total vcore-seconds taken by all map tasks=143729
 Total megabyte-seconds taken by all map tasks=294356992
 Map-Reduce Framework
 Map input records=5980598
 Map output records=0
 Input split bytes=75
 Spilled Records=0
 Failed Shuffles=0
 Merged Map outputs=0
 GC time elapsed (ms)=581
 CPU time spent (ms)=79020
 Physical memory (bytes) snapshot=477769728
 Virtual memory (bytes) snapshot=3041820672
 Total committed heap usage (bytes)=191365120
 GeoHashLib.Hbase.HbaseScannerTest$MyMapper$Counters
 KEYWORDS=288
 ROWS=5980598
 File Input Format Counters
 Bytes Read=0
 File Output Format Counters
 Bytes Written=0
 */
public class HbaseScannerTest {
        public static class MyReducer extends TableReducer<ImmutableBytesWritable,Put, ImmutableBytesWritable> {

            @Override
            protected void reduce(ImmutableBytesWritable rowkey,
                                  Iterable<Put> values,
                                  Context context) throws IOException, InterruptedException {
                Iterator<Put> i = values.iterator();
                if (i.hasNext()) {
                    context.write(rowkey, i.next());
                }

            }
        }
    public static class MyMapper extends TableMapper<Text, LongWritable> {
        enum Counters{ROWS, KEYWORDS};
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
//    public static class MyReducer extends TableReduce<ImmutableBytesWritable, Put, ImmutableBytesWritable>

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
//        Configuration conf = new Configuration();
       Configuration conf = HBaseConfiguration.create();
//        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("hadoop.home.dir", "/usr/lib/hadoop-2.6.3");
        conf.set("user","hadoop");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "3072");
        conf.set("mapreduce.map.java.opts", "-Xmx1024m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");

        TableMapReduceUtil.addDependencyJars(conf, MyMapper.class);

        Job job = new Job(conf, "KeyWords Counter");

        job.setUser("hadoop");

        job.setJarByClass(HbaseScannerTest.class);

        Scan scan = new Scan();
        scan.addColumn(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL);
        scan.setCaching(5000);

        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                CheckinDAO.FAMILY_NAME,
                CheckinDAO.CONTENT_COL,
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new SubstringComparator("李志"));

        scan.setFilter(scvf);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableMapperJob(
                Bytes.toString(CheckinDAO.TABLE_NAME),
                scan,
                MyMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job,
                true //addDependencyJars upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
