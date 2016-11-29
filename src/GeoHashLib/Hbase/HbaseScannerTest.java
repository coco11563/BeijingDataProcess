package GeoHashLib.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
        job.setJarByClass(ScanMapper.class);
        Scan scan = new Scan();
        scan.addColumn(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL);
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toString(CheckinDAO.TABLE_NAME),
                scan,
                ScanMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job
                //true //addDependencyJars upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
