package GeoHashLib.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static GeoHashLib.Hbase.CheckinDAO.CFG;
import static sql.jdbcConnector.getKeyWordNum;

/**
 * Created by Administrator on 2016/11/24.
 * keyword grep:
 * in 8 :
 * number of key words is 1619
 * all done in 68290 ms
 * in 16 :
 * number of key words is 1619
 * all done in 69235 ms
 * in 2 :
 * number of key words is 1619
 * all done in 67513 ms
 * all row scan:
 * in 2 :
 * number of key words is 5980598
 * all done in 183419 ms
 * in 16 :
 * number of key words is 5980598
 * all done in 184611 ms
 */
public class poorScaner {
    public poorScaner(int numSplit) {
        poorScaner.numSplit = numSplit;
    }
    private static int numSplit = 16;

    /**
     * number of key words is 1618
     * Mysql all done in 603233 ms
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void SqlMaker() throws ExecutionException, InterruptedException {
        Split[] splits = split(1, 6000000, numSplit);
        List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
        ExecutorService es = Executors.newFixedThreadPool(numSplit);
        for (final Split split : splits) {
            System.out.print(split.end + "." + split.start);
            workers.add(es.submit(() -> getKeyWordNum("fuck",split.end - split.start, split.start)));
    }
        int resultnum = 0;
        for (Future<?> f : workers) {
            resultnum += Integer.parseInt(f.get().toString());
        }
        System.out.println("number of key words is "+resultnum);
        es.shutdown();
    }
    public void workMaker() throws IOException, ExecutionException, InterruptedException {
        Split[] splits = split(1, 6000000, numSplit);
        List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
        ExecutorService es = Executors.newFixedThreadPool(numSplit);
        for (final Split split : splits) {
            workers.add(es.submit(() -> {
                System.out.print(split.end + "." + split.start);
                Connection connection = ConnectionFactory.createConnection(CFG);
                Table table = connection.getTable(TableName.valueOf("checkinInform"));
                int ret = 0;
                Scan s = new Scan(Bytes.toBytes(split.start), Bytes.toBytes(split.end));//zhege zenme ban!
                s.setCaching(500);
                    SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                            CheckinDAO.FAMILY_NAME,
                            CheckinDAO.CONTENT_COL,
                            CompareFilter.CompareOp.GREATER_OR_EQUAL,
                            new SubstringComparator("fuck"));
                    s.setFilter(scvf);
                try {
                    ResultScanner rs = table.getScanner(s);
                    for (Result r : rs) {
                            String rg = Bytes.toString(r.getValue(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL));
//                            System.out.println(rg)  ;
                        ret += rg.split("fuck").length - 1;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    table.close();
                    connection.close();
                }
                return ret;
            }));

        }
        int resultnum = 0;
        for (Future<?> f : workers) {
            resultnum += Integer.parseInt(f.get().toString());
        }
        System.out.println("number of key words is "+resultnum);
        es.shutdown();
    }
    private static final byte[] POSTFIX = new byte[] { 0x00 };

    public static Split[] split(int startrow, int endrow, int splitNum) {
        Split[] ret = new Split[splitNum];
        int per = (endrow - startrow) / splitNum;
        for (int i = 0 ; i < splitNum - 1 ; i ++) {
            ret[i] = new Split(startrow, startrow + per);
            startrow += per;
        }
        ret[splitNum - 1] = new Split(startrow, endrow);
        return ret;
    }
    public static void countRows(String tableName,byte[] lastRow) throws IOException {
        Split[] splits = split(1, 6000000, numSplit);
        Connection connection = ConnectionFactory.createConnection(CFG);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Filter filter = new PageFilter(15);
        int totalRows = 0;
        int i = 0;
        while(true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
//                System.out.println("Start row:" + Bytes.toStringBinary(startRow) + ", " + Bytes.toInt(startRow));
                scan.setStartRow(startRow);
            }
            ResultScanner  scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
//                System.out.println(localRows ++ + ": " + result);
                localRows += 1;
                totalRows += 1;
                lastRow = result.getRow();
                if (totalRows == splits[i].end) {
                    i ++;
                    System.out.println("====----dangdangdang----====" + i);
                    System.out.println(result.getColumnCells(CheckinDAO.FAMILY_NAME,CheckinDAO.ID_COL));
                }
            }
            scanner.close();
            if (localRows == 0) break;
        }
        System.out.println("total rows " + totalRows);
        table.close();
        connection.close();
    }
    public static void useGetter() {
        Configuration conf = HBaseConfiguration.create();
//        Get getter = new Get(conf);
    }
    public static void main(String args[]) throws InterruptedException, ExecutionException, IOException {
//        long start = System.currentTimeMillis();
//        poorScaner p = new poorScaner(16);
//        p.workMaker();
//        long end = System.currentTimeMillis();
//        System.out.println("HBase all done in " + (end - start) + " ms");
//        start = System.currentTimeMillis();
//        p = new poorScaner(8);
//        p.SqlMaker();
//        end = System.currentTimeMillis();
//        System.out.println("Mysql all done in " + (end - start) + " ms");
        countRows("checkinInform",Bytes.toBytes(5980590));
    }

}
