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
    String[] countDownRow = {
            "w7ujumhbbk8b_3746905597555205",
            "wx4dhwcevbgm_3732318542090834",
            "wx4dxvc335ng_3796718757622951",
            "wx4ehjg49z89_3941008133360339",
            "wx4eqcfhnrwy_3892193137449120",
            "wx4ev93hz7c4_3745229091374348",
            "wx4f2nejy9f9_3874055109193806",
            "wx4fc5jtp6nw_3745958821294909",
            "wx4fkxjtts2q_3852854441944366",
            "wx4g15ss8h87_3803303747923481",
            "wx4g3ufdcby9_3854120517658550",
            "wx4g8cty8dqq_3798677887575440",
            "wx4gj1chv1tk_3807164386447589",
            "wx4qp31t2jje_3809720952970807",
            "wx4sv6961xg1_3862440414183800",
            "wx4uy2rynsfr_3764917620017549",
            "wx6by685umvg_3729233677080563",
            "wx6by685umvg_3729233677080563"
    };
    /**
     * number of key words is 1618
     * Mysql all done in 603233 ms
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void SqlMaker(String key) throws ExecutionException, InterruptedException {
        Split[] splits = split(1, 6000000, numSplit);
        List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
        ExecutorService es = Executors.newFixedThreadPool(numSplit);
        for (final Split split : splits) {
            System.out.print(split.end + "." + split.start);
            workers.add(es.submit(() -> getKeyWordNum(key,split.end - split.start, split.start)));
    }
        int resultnum = 0;
        for (Future<?> f : workers) {
            resultnum += Integer.parseInt(f.get().toString());
        }
        System.out.println("number of key words is "+resultnum);
        es.shutdown();
    }
    public void workMaker(String key, int sign) throws IOException, ExecutionException, InterruptedException {
//        switch (sign) {
//            case 1: {
//                Split[] splits = split(1, 6000000, numSplit);
//                List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
//                ExecutorService es = Executors.newFixedThreadPool(numSplit);
//                for (final Split split : splits) {
//                    workers.add(es.submit(() -> {
//                        System.out.print(split.end + "." + split.start);
//                        Connection connection = ConnectionFactory.createConnection(CFG);
//                        Table table = connection.getTable(TableName.valueOf("checkinInform"));
//                        int ret = 0;
//                        Scan s = new Scan(Bytes.toBytes(split.start), Bytes.toBytes(split.end));//zhege zenme ban!
//                        s.setCaching(500);
//                        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
//                                CheckinDAO.FAMILY_NAME,
//                                CheckinDAO.CONTENT_COL,
//                                CompareFilter.CompareOp.GREATER_OR_EQUAL,
//                                new SubstringComparator(key));
//                        s.setFilter(scvf);
//                        try {
//                            ResultScanner rs = table.getScanner(s);
//                            for (Result r : rs) {
//                                String rg = Bytes.toString(r.getValue(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL));
////                            System.out.println(rg)  ;
//                                ret += rg.split(key).length - 1;
//                            }
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        } finally {
//                            table.close();
//                            connection.close();
//                        }
//                        return ret;
//                    }));
//                }
//                int resultnum = 0;
//                for (Future<?> f : workers) {
//                    resultnum += Integer.parseInt(f.get().toString());
//                    System.out.println("number of key words is " + resultnum);
//                }
//                es.shutdown();
//            }
//            break;
//            case 2: {
                List<Future<?>> workers = new ArrayList<Future<?>>(numSplit);
                ExecutorService es = Executors.newFixedThreadPool(numSplit);
                for (int i = 0 ; i < countDownRow.length - 1 ; i ++) {
                    final String startl = countDownRow[i];
                    final String endl = countDownRow[i + 1];
                    workers.add(es.submit(() -> {
                        Connection connection = ConnectionFactory.createConnection(CFG);
                        Table table = connection.getTable(TableName.valueOf("checkinInform"));
                        int ret = 0;
                        System.out.println(startl + " - " + endl);
                        Scan s = new Scan(Bytes.toBytes(startl),Bytes.toBytes(endl));//zhege zenme ban!
                        s.setCaching(5000);
                        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                                CheckinDAO.FAMILY_NAME,
                                CheckinDAO.CONTENT_COL,
                                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                                new SubstringComparator(key));
                        s.setFilter(scvf);
                        try {
                            ResultScanner rs = table.getScanner(s);
                            for (Result r : rs) {
                                String rg = Bytes.toString(r.getValue(CheckinDAO.FAMILY_NAME, CheckinDAO.CONTENT_COL));
//                            System.out.println(rg)  ;
                                ret += rg.split(key).length - 1;
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
//                    System.out.println("number of key words is " + resultnum);
                }
                System.out.println("number of key words is " + resultnum);
                es.shutdown();
//            }
//            break;
//        }
    }
    private static final byte[] POSTFIX = new byte[] { 0x00 };

    /**
     * start [w7ujumhbbk8b_3746905597555205/sinaWeibo:idstr/1480334437128/Put/vlen=16/seqid=0]
     * [wx4dhwcevbgm_3732318542090834/sinaWeibo:idstr/1480334602323/Put/vlen=16/seqid=0]
     * [wx4dxvc335ng_3796718757622951/sinaWeibo:idstr/1480334775419/Put/vlen=16/seqid=0]
     * [wx4ehjg49z89_3941008133360339/sinaWeibo:idstr/1480334942748/Put/vlen=16/seqid=0]
     * [wx4eqcfhnrwy_3892193137449120/sinaWeibo:idstr/1480335124013/Put/vlen=16/seqid=0]
     * [wx4ev93hz7c4_3745229091374348/sinaWeibo:idstr/1480335288027/Put/vlen=16/seqid=0]
     * [wx4f2nejy9f9_3874055109193806/sinaWeibo:idstr/1480335472572/Put/vlen=16/seqid=0]
     * [wx4fc5jtp6nw_3745958821294909/sinaWeibo:idstr/1480335644156/Put/vlen=16/seqid=0]
     * [wx4fkxjtts2q_3852854441944366/sinaWeibo:idstr/1480335818309/Put/vlen=16/seqid=0]
     * [wx4g15ss8h87_3803303747923481/sinaWeibo:idstr/1480336024399/Put/vlen=16/seqid=0]
     * [wx4g3ufdcby9_3854120517658550/sinaWeibo:idstr/1480336187786/Put/vlen=16/seqid=0]
     * [wx4g8cty8dqq_3798677887575440/sinaWeibo:idstr/1480336371106/Put/vlen=16/seqid=0]
     * [wx4gj1chv1tk_3807164386447589/sinaWeibo:idstr/1480336543273/Put/vlen=16/seqid=0]
     * [wx4qp31t2jje_3809720952970807/sinaWeibo:idstr/1480336778093/Put/vlen=16/seqid=0]
     * [wx4sv6961xg1_3862440414183800/sinaWeibo:idstr/1480336953963/Put/vlen=16/seqid=0]
     * [wx4uy2rynsfr_3764917620017549/sinaWeibo:idstr/1480337280194/Put/vlen=16/seqid=0]
     * [wx6by685umvg_3729233677080563/sinaWeibo:idstr/1480337448227/Put/vlen=16/seqid=0]
     * [wx4dhwcevbgm_3732318542090834/sinaWeibo:idstr/1480334602323/Put/vlen=16/seqid=0]
     * [wx4dxvc335ng_3796718757622951/sinaWeibo:idstr/1480334775419/Put/vlen=16/seqid=0]
     * [wx4ehjg49z89_3941008133360339/sinaWeibo:idstr/1480334942748/Put/vlen=16/seqid=0]
     * [wx4eqcfhnrwy_3892193137449120/sinaWeibo:idstr/1480335124013/Put/vlen=16/seqid=0]
     * [wx4ev93hz7c4_3745229091374348/sinaWeibo:idstr/1480335288027/Put/vlen=16/seqid=0]
     * [wx4f2nejy9f9_3874055109193806/sinaWeibo:idstr/1480335472572/Put/vlen=16/seqid=0]
     * [wx4fc5jtp6nw_3745958821294909/sinaWeibo:idstr/1480335644156/Put/vlen=16/seqid=0]
     * [wx4fkxjtts2q_3852854441944366/sinaWeibo:idstr/1480335818309/Put/vlen=16/seqid=0]
     * [wx4g15ss8h87_3803303747923481/sinaWeibo:idstr/1480336024399/Put/vlen=16/seqid=0]
     * [wx4g3ufdcby9_3854120517658550/sinaWeibo:idstr/1480336187786/Put/vlen=16/seqid=0]
     * [wx4g8cty8dqq_3798677887575440/sinaWeibo:idstr/1480336371106/Put/vlen=16/seqid=0]
     * [wx4gj1chv1tk_3807164386447589/sinaWeibo:idstr/1480336543273/Put/vlen=16/seqid=0]
     * [wx4qp31t2jje_3809720952970807/sinaWeibo:idstr/1480336778093/Put/vlen=16/seqid=0]
     * [wx4sv6961xg1_3862440414183800/sinaWeibo:idstr/1480336953963/Put/vlen=16/seqid=0]
     * [wx4uy2rynsfr_3764917620017549/sinaWeibo:idstr/1480337280194/Put/vlen=16/seqid=0]
     * [wx6by685umvg_3729233677080563/sinaWeibo:idstr/1480337448227/Put/vlen=16/seqid=0]
     *  end:[wx6by685umvg_3729233677080563/sinaWeibo:idstr/1480337448227/Put/vlen=16/seqid=0]
     *
     * "w7ujumhbbk8b_3746905597555205",
     * "wx4dhwcevbgm_3732318542090834",
     * "wx4dxvc335ng_3796718757622951",
     * "wx4ehjg49z89_3941008133360339",
     * "wx4eqcfhnrwy_3892193137449120",
     * "wx4ev93hz7c4_3745229091374348",
     * "wx4f2nejy9f9_3874055109193806",
     * "wx4fc5jtp6nw_3745958821294909",
     * "wx4fkxjtts2q_3852854441944366",
     * "wx4g15ss8h87_3803303747923481",
     * "wx4g3ufdcby9_3854120517658550",
     * "wx4g8cty8dqq_3798677887575440",
     * "wx4gj1chv1tk_3807164386447589",
     * "wx4qp31t2jje_3809720952970807",
     * "wx4sv6961xg1_3862440414183800",
     * "wx4uy2rynsfr_3764917620017549",
     * "wx6by685umvg_3729233677080563",
     * "wx6by685umvg_3729233677080563"
     * @param startrow
     * @param endrow
     * @param splitNum
     * @return
     */
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
        Split[] splits = split(1, 5980590, numSplit);
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
                if (totalRows == 5980590 || totalRows == 1) {
                    System.out.println("====----First and End----====" );
                    System.out.println(result.getColumnCells(CheckinDAO.FAMILY_NAME,CheckinDAO.ID_COL));
                }
                if (i < 16 && totalRows == splits[i].start ) {
                    System.out.println("====----dangdangdang----====" + i);
                    System.out.println(result.getColumnCells(CheckinDAO.FAMILY_NAME,CheckinDAO.ID_COL));
                    i += 1;
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
        long start = System.currentTimeMillis();
        poorScaner p = new poorScaner(16);
        p.workMaker( "李志",2);
        long end = System.currentTimeMillis();
        System.out.println("HBase all done in " + (end - start) + " ms");
        start = System.currentTimeMillis();
        p = new poorScaner(8);
        p.SqlMaker("李志");
        end = System.currentTimeMillis();
        System.out.println("Mysql all done in " + (end - start) + " ms");



//        countRows("checkinInform",Bytes.toBytes(5980590));
    }

}
