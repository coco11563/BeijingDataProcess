package GeoHashLib.Hbase;

import GeoHashLib.GeoHash;
import com.google.common.collect.MinMaxPriorityQueue;
import com.mysql.jdbc.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static GeoHashLib.Hbase.CheckinDAO.CFG;
import static GeoHashLib.Hbase.CheckinDAO.TABLE_NAME;
import static GeoHashLib.Hbase.poorScaner.numSplit;
import static org.apache.hadoop.hbase.TableName.*;
import static sql.jdbcConnector.getConn;
import static sql.jdbcConnector.getKeyWordNum;

/**
 * Created by root on 12/16/16.
 */
public class GeohaseQuery {
    static class QueryMatch {
        public String id;
        public String hash;
        public double lon, lat;
        public double distance = Double.NaN;
        public String content;
        public QueryMatch(String id, String hash, double lon, double lat) {
            this.id = id;
            this.hash = hash;
            this.lon = lon;
            this.lat = lat;
        }
        public QueryMatch(String id, String hash, double lon, double lat, String content) {
            this.id = id;
            this.hash = hash;
            this.lon = lon;
            this.lat = lat;
            this.content = content;
        }
    }

    static class DistanceComparator implements Comparator<QueryMatch> {
        DistanceComparator(double lon, double lat) {
            this.orgin = new Point2D.Double(lon, lat);
        }
        Point2D orgin;
        @Override
        public int compare(QueryMatch o1, QueryMatch o2) {
            if (Double.isNaN(o1.distance)) {
            o1.distance = orgin.distance(o1.lon, o1.lat);
            }
            if (Double.isNaN(o2.distance)) {
                o1.distance = orgin.distance(o2.lon, o2.lat);
            }
            return Double.compare(o1.distance, o2.distance);
        }
    }
    static List<QueryMatch> takeN(String prefix,String key) throws IOException {
        Connection connection = ConnectionFactory.createConnection(CFG);
        List<QueryMatch> candidates = new ArrayList<>();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new RegexStringComparator("^" + prefix));
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                CheckinDAO.FAMILY_NAME,
                CheckinDAO.CONTENT_COL,
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new SubstringComparator(key));
        fl.addFilter(rowFilter);
        fl.addFilter(scvf);

        Scan scan = new Scan(prefix.getBytes());
        scan.setFilter(fl);

        scan.addFamily(CheckinDAO.FAMILY_NAME);
        scan.setCaching(500);

        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        ResultScanner scanner = table.getScanner(scan);

        for (Result r : scanner) {
            String hash = new String(r.getRow());
            String id = new String(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.ID_COL));
            String content = Bytes.toString(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.CONTENT_COL));
            candidates.add(new QueryMatch(id, hash, 0.0, 0.0, content));
        }
        scanner.close();
        table.close();
        connection.close();
        return candidates;
    }

    static Collection<QueryMatch> takeNMuiltiThread(Comparator<QueryMatch> comp,
                                                    String prefix,
                                                    int n,
                                                    String startrow,
                                                    String endrow) throws IOException {
        Collection<QueryMatch> candidates = MinMaxPriorityQueue.orderedBy(comp).maximumSize(n).create();

        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new RegexStringComparator(prefix+"*"));

        Scan scan = new Scan(startrow.getBytes(), endrow.getBytes());
        scan.setFilter(rowFilter);

        scan.addFamily(CheckinDAO.FAMILY_NAME);
        scan.setCaching(5000);

        Table table = ConnectionFactory.createConnection(CFG).getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner scanner = table.getScanner(scan);

        for (Result r : scanner) {
            String hash = new String(r.getRow());
            String id = new String(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.ID_COL));
            double lon = Bytes.toDouble(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.LNG_COL));
            double lat = Bytes.toDouble(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.LAT_COL));
            String content = Bytes.toString(r.getValue(CheckinDAO.FAMILY_NAME,CheckinDAO.CONTENT_COL));
            candidates.add(new QueryMatch(id, hash, lon, lat, content));
        }
        table.close();
        return candidates;
    }

    public static int query(double lat, double lon, String keyword, int scale) throws IOException, InterruptedException, ExecutionException {
        Connection connection = ConnectionFactory.createConnection(CFG);
        List<Future<?>> workers = new ArrayList<Future<?>>(9);
        ExecutorService es = Executors.newFixedThreadPool(9);
        int counter = 0;
        DistanceComparator distanceComparator = new DistanceComparator(lon, lat);
        List<QueryMatch> ret = new LinkedList<>();

        GeoHash target = new GeoHash(lat, lon, scale);

        List<String> setUp = new LinkedList<>();

        setUp.addAll(target.getGeoHashBase32For9());

        for (String s : setUp) {
            workers.add(es.submit(() -> {
                return takeN(s, keyword);
            }));
        }
        for (Future<?> f : workers) {
            ret.addAll((Collection<? extends QueryMatch>) f.get());
        }
        es.shutdown();
        connection.close();
        for (QueryMatch qm : ret) {
            counter += containsKeywords(qm.content, keyword);
        }

        return counter;
    }
    private static int containsKeywords(String msg, String keyword) {
        String[] ress  = msg.split(keyword);
        return ress.length - 1;
    }
    public static int muiltiThreadQuery (double lat, double lon, int n, String keyword, String startrow, String endrow) throws IOException {
        int counter = 0;
        DistanceComparator distanceComparator = new DistanceComparator(lon, lat);
        List<QueryMatch> ret = new LinkedList<>();

        GeoHash target = new GeoHash(lat, lon, 7);
        ret.addAll(takeNMuiltiThread(distanceComparator, target.getGeoHashBase32(), n, startrow, endrow));
        for (String s : target.getGeoHashBase32For9()) {
            ret.addAll(takeNMuiltiThread(distanceComparator, s, n, startrow, endrow));
        }
        for (QueryMatch qm : ret) {
            counter += containsKeywords(qm.content, keyword);
        }
        return counter;
    }
    public static int sqlQuery(double lat, double lon , String keyword, int scale) throws ExecutionException, InterruptedException {
        List<Future<?>> workers = new ArrayList<Future<?>>(9);
        ExecutorService es = Executors.newFixedThreadPool(9);
        int counter = 0;

        List<QueryMatch> ret = new LinkedList<>();

        GeoHash target = new GeoHash(lat, lon, scale);

        List<String> setUp = new LinkedList<>();

        setUp.addAll(target.getGeoHashBase32For9());

        for (String s : setUp) {
            workers.add(es.submit(() -> {
                return getKeyWordNum(s, keyword);
            }));
        }
        for (Future<?> f : workers) {
            int n = (Integer) f.get();
            counter += n;
        }
        es.shutdown();
        return counter;
    }
    /**
     *
     * scale : 7 double Filter -> 42365ms(600w) - 62753ms(1100w) - 276
     * scale : 7 Mysql -> 3588ms(600w) - 3444ms(1100w) - 270
     *
     * scale : 6 double Filter -> 53043ms(600w) - 80963ms(1100w) - 2999
     * scale : 6 Msyql -> 4568ms(600w) - 3892ms(1100w) -> 2964个
     *
     * scale : 5 single Filter -> 162179ms(600w)- 226570ms(1100w),260255ms,232864ms,188434ms - 79099
     * scale : 5 Mysql -> 29814ms(600w)36614 - 35582ms(1100w) - 75697
     *
     * scale : 4 double Filter -> 179260ms(600w) - 238888ms(1100w) - 291231 , 293858
     * scale : 4 Mysql -> 214472ms(600w) - 192484ms(1100w) - 286328 , 283748
     *
     * scale : 3 double Filter -> 308301ms(600w) - 313344ms(1100w) - 341051个
     * scale : 3 Mysql -> 400452ms(600w) - 394581ms(1100w) - 333694个
     *
     * scale : 2 double Filter -> 308575ms 346397
     * scale : 2 Mysql -> 463649ms 338174
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static  void main(String args[]) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        int i = query(39.9208460000,116.4165150000, "北京", 7);
        long end = System.currentTimeMillis();
        System.out.println(end - start + "ms");
        System.out.println(i + "个");
//        long start_1 = System.currentTimeMillis();
//        int j = sqlQuery(39.9208460000,116.4165150000, "北京", 5);
//        long end_1 = System.currentTimeMillis();
//        System.out.println(end_1 - start_1 + "ms");
//        System.out.println(j + "个");
    }

}
