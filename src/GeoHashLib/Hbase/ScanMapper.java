package GeoHashLib.Hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by hadoop on 2016/11/29.
 */
public class ScanMapper extends TableMapper<Text, LongWritable> {
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