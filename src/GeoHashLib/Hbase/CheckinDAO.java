package GeoHashLib.Hbase;

import GeoHashLib.GeoHash;
import dataRead.CheckIn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by coco1 on 2016/11/23.
 *
 */
class CheckinDAO {
    private static LinkedList<Put> puts = new LinkedList<>();
    static final Configuration CFG = HBaseConfiguration.create();
    static final byte[] TABLE_NAME = Bytes.toBytes("checkinInform");
    static final byte[] FAMILY_NAME = Bytes.toBytes("sinaWeibo");
    static final byte[] ID_COL = Bytes.toBytes("idstr");
    private static final byte[] LAT_COL = Bytes.toBytes("lat");
    private static final byte[] LNG_COL = Bytes.toBytes("lng");
    private static final byte[] POIID_COL = Bytes.toBytes("poiid");
    private static final byte[] CLOCK_COL = Bytes.toBytes("clock");
    private static final byte[] DATETIME_COL = Bytes.toBytes("datetime");
    static final byte[] CONTENT_COL = Bytes.toBytes("content");

    private Connection conn;

    public CheckinDAO(Connection connection){
        this.conn = connection;
    }
    CheckinDAO() throws IOException {
        this.conn = ConnectionFactory.createConnection(CFG);
    }
    private static Get mkGet(String id) {
        Get g = new Get(Bytes.toBytes(id));
        g.addFamily(FAMILY_NAME);
        return g;
    }
    private static Put mkPut(CheckIn checkIn) {
//        System.out.println(Bytes.toString(mkIdstr(checkIn)) + "-" + checkIn.toString());
        Put p = new Put(mkIdstr(checkIn));
        p.addColumn(FAMILY_NAME,ID_COL,Bytes.toBytes(checkIn.getIdstr()));
        p.addColumn(FAMILY_NAME, LAT_COL, Bytes.toBytes(checkIn.getLat()));
        p.addColumn(FAMILY_NAME, LNG_COL, Bytes.toBytes(checkIn.getLng()));
        p.addColumn(FAMILY_NAME, POIID_COL, Bytes.toBytes(checkIn.getPoiid()));
        p.addColumn(FAMILY_NAME, CLOCK_COL, Bytes.toBytes(checkIn.getTime()));
        p.addColumn(FAMILY_NAME, DATETIME_COL, Bytes.toBytes(checkIn.getDate()));
        p.addColumn(FAMILY_NAME, CONTENT_COL, Bytes.toBytes(checkIn.getContent()));
        return p;
    }
    private static Delete mkDel(String id) {
        return new Delete(Bytes.toBytes(id));
    }
    private static byte[] mkIdstr(CheckIn checkIn) {
        return Bytes.toBytes(new GeoHash(
                Double.parseDouble(checkIn.getLat()),
                Double.parseDouble(checkIn.getLng()))
                .getGeoHashBase32() + "_" + checkIn.getIdstr());
    }

    private static byte[] mkIdstr(double lat, double lng, String idstr ) {
        return Bytes.toBytes(new GeoHash(lat,lng).getGeoHashBase32() + "_" + idstr);
    }

    public void put(List<Put> put, Configuration cfg, Connection conn, TableName tableName) {
        try {
            Table table = conn.getTable(tableName);
            table.put(put);
            Thread.sleep(1000);
            table.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    void addCheckin(CheckIn c) {
        puts.add(mkPut(c));
        if (puts.size() > 10000) {
            put(puts, CFG, conn, TableName.valueOf(TABLE_NAME));
            puts = new LinkedList<>();
        }
    }

    private static class Checkin extends CheckIn {
        private Checkin(Result result) {
            this(result.getValue(FAMILY_NAME,ID_COL),
                    result.getValue(FAMILY_NAME, CONTENT_COL),
                    result.getValue(FAMILY_NAME, LAT_COL),
                    result.getValue(FAMILY_NAME, LNG_COL),
                    result.getValue(FAMILY_NAME, POIID_COL),
                    result.getValue(FAMILY_NAME, DATETIME_COL),
                    result.getValue(FAMILY_NAME, CLOCK_COL));
        }
        private Checkin(byte[] idstr, byte[] content, byte[] lat,
                        byte[] lng, byte[] poiid, byte[] date, byte[] clock) {
            this(Bytes.toString(idstr),Bytes.toString(content),
                    Bytes.toString(lat), Bytes.toString(lng),
                    Bytes.toString(poiid),  Bytes.toString(date),
                    Integer.parseInt(Bytes.toString(clock)));
        }
        private Checkin(String idstr, String content, String lat,
                        String lng, String poiid, String date, int clock) {
            super(idstr, content, lat, lng, poiid, date, clock);
        }
    }
}
