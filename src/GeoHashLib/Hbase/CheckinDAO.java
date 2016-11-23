package GeoHashLib.Hbase;

import GeoHashLib.GeoHash;
import dataRead.CheckIn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by coco1 on 2016/11/23.
 *
 */
public class CheckinDAO {
    private static final LinkedList<Put> puts = new LinkedList<>();
    private static final Configuration CFG = HBaseConfiguration.create();
    private static final byte[] TABLE_NAME = Bytes.toBytes("checkinInform");
    private static final byte[] FAMILY_NAME = Bytes.toBytes("sinaWeibo");
    private static final byte[] ID_COL = Bytes.toBytes("idstr");
    private static final byte[] LAT_COL = Bytes.toBytes("lat");
    private static final byte[] LNG_COL = Bytes.toBytes("lng");
    private static final byte[] POIID_COL = Bytes.toBytes("poiid");
    private static final byte[] CLOCK_COL = Bytes.toBytes("clock");
    private static final byte[] DATETIME_COL = Bytes.toBytes("datetime");
    private static final byte[] CONTENT_COL = Bytes.toBytes("content");

    private Connection conn;

    public CheckinDAO(Connection connection){
        this.conn = connection;
    }
    public CheckinDAO() throws IOException {
        this.conn = ConnectionFactory.createConnection(CFG);
    }
    private static Get mkGet(String id) {
        Get g = new Get(Bytes.toBytes(id));
        g.addFamily(FAMILY_NAME);
        return g;
    }
    private static Put mkPut(CheckIn checkIn) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void addCheckin(CheckIn c) {
        puts.add(mkPut(c));
        if (puts.size() > 1000) {
            put(puts, CFG, conn, TableName.valueOf(TABLE_NAME));
            puts.clear();
        }
    }
}
