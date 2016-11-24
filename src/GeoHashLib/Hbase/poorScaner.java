package GeoHashLib.Hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static GeoHashLib.Hbase.CheckinDAO.CFG;

/**
 * Created by Administrator on 2016/11/24.
 */
public class poorScaner {
    private static int numSplit = 8;
    public void workMaker(int startrow, int endrow) throws IOException, ExecutionException, InterruptedException {
        Split[] splits = split(startrow, endrow, numSplit);
        List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
        ExecutorService es = Executors.newFixedThreadPool(numSplit);
        for (final Split split : splits) {
            workers.add(es.submit(new Runnable() {
                Connection connection = ConnectionFactory.createConnection(CFG);
                Table table = connection.getTable(TableName.valueOf(""));
                @Override
                public void run() {
                    Scan s = new Scan(Bytes.toBytes(split.start), Bytes.toBytes(split.end));
                    try {
                        ResultScanner rs = table.getScanner(s);
                        for (Result r : rs) {

                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }));

        }
        for (Future<?> f : workers) {
            f.get();
        }
        es.shutdown();
    }

    public Split[] split(int startrow, int endrow, int splitNum) {
        Split[] ret = new Split[splitNum];
        int per = (startrow - endrow) / splitNum;
        for (int i = 0 ; i < splitNum - 1 ; i ++) {
            ret[i] = new Split(startrow, startrow + per);
            startrow += per;
        }
        ret[splitNum] = new Split(startrow, endrow);
        return ret;
    }
}
