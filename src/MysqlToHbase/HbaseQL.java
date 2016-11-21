package MysqlToHbase;

import dataRead.CheckIn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by coco1 on 2016/11/18.
 *
 * 提供了一部分基础的hbase操作方法
 *
 */
public class HbaseQL {
    public static final String tableName = "checkinInform";
    private static Logger logger = Logger.getLogger(HbaseQL.class);
    public static Configuration cfg = HBaseConfiguration.create();
    public static final String columnFamily = "beijing_check_in";
    static HTable getTable() throws IOException {
        return new HTable(cfg, tableName);
    }
    public static void create(String tableName)throws IOException {
        logger.info("正在创建"+tableName);
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
        }
        admin.close();
    }

    public static void create(String tablename,String columnFamily) throws Exception {
        @SuppressWarnings({ "resource", "deprecation" })
        HBaseAdmin admin = new HBaseAdmin(cfg);
        System.out.print(cfg);
        if (admin.tableExists(Bytes.toBytes(tablename))) {
            logger.error("table Exists!");
        }
        else{
            logger.info("create table . . .");
            @SuppressWarnings("deprecation")
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            if (admin.tableExists(Bytes.toBytes(tablename))) {
                logger.info("create table success!");
            }

        }
    }

    /**
     *
     * @param tablename 表名
     * @param row 列名
     * @param columnFamily cF
     * @param column 数据名
     * @param data 数据
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    public static void put(String tablename, String row, String columnFamily, String column, String data)
            throws Exception {
        @SuppressWarnings({ "resource" })
        HTable table = new HTable(cfg, tablename);
        Put p1 = new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(p1);
        logger.info("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
    }

    /**
     *
     * @param tablename 表名
     * @param row 行名
     * @param columnFamily 列组
     * @param array mapper值对键
     * @return put
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    public static Put put(String tablename, String row,String columnFamily,HashMap<String,String> array )
            throws Exception {
        Put p1 = new Put(Bytes.toBytes(row));
        Set<String> keyset = array.keySet();
        for (String key : keyset) {
            if (array.get(key) != null) {
                p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(array.get(key)));
            } else {
                logger.info("好險，避免了一次空指針");
            }
        }
        return p1;
    }public static Put put(String tablename, String row,String columnFamily,CheckIn c)
            throws Exception {
        return put(tablename, c.getIdstr(), columnFamily, mapMaker(c));
    }

    public static Put put(CheckIn c) throws Exception {
            return put(tableName, c.getIdstr(), columnFamily, mapMaker(c));
    }
    public static HashMap<String, String> mapMaker(CheckIn c) {
        HashMap<String, String> hm = new HashMap<>();
        hm.put("content", c.getContent());
        hm.put("lat", c.getLat());
        hm.put("lng", c.getLng());
        hm.put("datetime", c.getDate());
        hm.put("idstr", c.getIdstr());
        hm.put("time", Integer.toString(c.getTime()));
        hm.put("poiid", c.getPoiid());
        return hm;
    }
    public static void get(String tablename, String row) throws IOException {
        @SuppressWarnings({ "deprecation", "resource" })
        HTable table = new HTable(cfg, tablename);
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
        logger.info("Get: " + result);
    }


    public static void scan(String tablename) throws Exception {
        @SuppressWarnings({ "deprecation", "resource" })
        HTable table = new HTable(cfg, tablename);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        for (Result r : rs) {
            logger.info("Scan: " + r);
        }
    }

    public static boolean delete(String tablename) throws IOException {

        @SuppressWarnings({ "deprecation", "resource" })
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }

        }
        return true;
    }
    //61233ms

    public static int getKeyValue(String key) throws IOException {
        long start = System.currentTimeMillis();
        HTable tb = new HTable(cfg, "checkinInform");
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                columnFamily.getBytes(),
                "content".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(key));
        scvf.setFilterIfMissing(false);
        scvf.setLatestVersionOnly(true); // OK
//        Filter kof = new KeyOnlyFilter(); // OK 返回所有的行，但值全是空
//        List<Filter> filters = new ArrayList<Filter>();
//        filters.add(kof);
//        filters.add(scvf);
//        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setFilter(scvf);
        ResultScanner rs = tb.getScanner(scan);
        int i = 0;
        for (Result r : rs) {
            String res = Bytes.toString(r.getValue(columnFamily.getBytes(), "content".getBytes()));
            String[] ress = res.split(key);
            i += ress.length - 1;
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        return i;
    }

    public static  void main(String args[]) throws IOException {
        System.out.println(getKeyValue("我爱你"));
        System.out.println(123123);
    }
}
