package sql;
import GeoHashLib.GeoHash;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import dataRead.CheckIn;
import sinaGrab.poiInForm;

import java.sql.BatchUpdateException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by coco1 on 2016/10/19.
 */
public class jdbcConnector {
    public static int dul = 0;
    public static int insert = 0;
    private static final String DATABASEADDRESS = "jdbc:mysql://192.168.1.80:3306/";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String insertSql  = "insert into RawData.checkin (checkin,lat,lng,poiid,clock,content,datetime,id) values(?,?,?,?,?,?,?,?)";
    public static final String insertPoiInform = "insert into checkInData.poiinform (poiid,lat,lng,type) values(?,?,?,?)";

    /**
     * 获取数据库连接
     *
     * @return 返回一个数据库连接 Connection
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName(DRIVER); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(DATABASEADDRESS, USER, PASSWORD);
            conn.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("can't connect to db");
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 直连
     * 只需要提供checkin数据
     * 这个方法会自动进行数据库连接
     * @param checkIn 签到数据
     * @return executeUpdate
     */
    public static int checkInInsert(CheckIn checkIn) {
        Connection conn = getConn();
        int i = 0;
        String sql = "insert into checkin (checkin,lat,lng,poiid,clock,content,id) values(?,?,?,?,?,?,?)";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            pstmt.setString(1, checkIn.getIdstr());
            pstmt.setString(2, checkIn.getLat());
            pstmt.setString(3, checkIn.getLng());
            pstmt.setString(4, checkIn.getPoiid());
            pstmt.setInt(5, checkIn.getTime());
            pstmt.setString(6, checkIn.getContent());
            pstmt.setString(7,mkId(checkIn));
            i = pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    private static String mkId(CheckIn c) {
        return new GeoHash(Double.parseDouble(c.getLat()),Double.parseDouble(c.getLng())).getGeoHashBase32() + "_" + c.getIdstr();
    }

    /**
     * 使用**数据库连接实例** 和 **preparestatement**进行连接的方法
     * 使用了批量插入的方法
     * @return
     */
    public static void checkInInsert(List<CheckIn> checkInList, Connection connection, PreparedStatement pstmt) throws SQLException {
        int i = 0;
        for (CheckIn checkIn : checkInList) {
            try {
                pstmt.setString(1, checkIn.getIdstr());
                pstmt.setString(2, checkIn.getLat());
                pstmt.setString(3, checkIn.getLng());
                pstmt.setString(4, checkIn.getPoiid());
                pstmt.setInt(5, checkIn.getTime());
                pstmt.setString(6, checkIn.getContent());
                pstmt.setString(7,checkIn.getDate());
                pstmt.setString(8,mkId(checkIn));
                pstmt.addBatch();
                i ++;
                if (i % 10000 == 0){
                    pstmt.executeBatch();
                    connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            pstmt.executeBatch();
            connection.commit();
        } catch (BatchUpdateException | MySQLIntegrityConstraintViolationException e ) {
            e.printStackTrace();
        }
    }

    /**
     * 向数据库中插入poi讯息的函数
     * 使用**数据库连接实例** 和 **preparestatement**进行连接的方法
     * 使用了批量插入的方法
     */
    public static void poiInformInsert(List<poiInForm> poiInFormsList, Connection connection, PreparedStatement pstmt) throws SQLException {
        int i = 0;
        System.out.println("插入" + poiInFormsList.size() + "条数据");
        for (poiInForm poi : poiInFormsList) {
            try {
                insert++;
                pstmt.setString(1, poi.getPoiid());
                pstmt.setString(2, poi.getLat());
                pstmt.setString(3, poi.getLon());
                pstmt.setString(4, poi.getType());
                pstmt.addBatch();
                i ++;
                if (i % 10000 == 0){
                    pstmt.executeBatch();
                    connection.commit();
                }
            } catch (SQLException e) {
                System.err.println(e.toString());
                e.printStackTrace();
            }
        }
        try {
            pstmt.executeBatch();
            connection.commit();
        } catch (BatchUpdateException e) {
            dul ++;
           System.err.println("重复的插入:" + dul + "/" + insert);
        }

    }

    /**
     * @param poiid 传入poiid讯息
     * @param connection 使用全局变量存储连接讯息
     * @return 是否存在这个poiid
     */
    public static Boolean have(String poiid, Connection connection) throws SQLException {
        PreparedStatement ps = null;
        int num = 0;
        String sql = "select count(*) from checkInData.poiinform a WHERE a.poiid=\'" + poiid+"\'LIMIT 1";
        try {
            ps = (PreparedStatement)connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) { //需要把游标移动到第一位
                num = rs.getInt(1);
            }
            if (num == 0) {
                return false;
            }
        } catch (SQLException e) {
            System.err.print("Wrong while query the num");
            e.printStackTrace();
        } finally {
            ps.close();
        }
        return true;
    }


    public static void delCheckin(String poiid, Connection connection) {
        PreparedStatement ps;
        String sql = "delete from checkInData.checkin WHERE poiid=\'" + poiid + "\'";
        try {
            ps = (PreparedStatement)connection.prepareStatement(sql);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.print("Wrong while query the num");
            e.printStackTrace();
        }
    }


    /**
     * 获取checkin中所有的poi讯息
     * @return List形式的String
     */

    public static List<String> getAllPoiid() {
        List<String> ret = new ArrayList<>();
        Connection conn = getConn();
        String sql = "select DISTINCT poiid from checkInData.checkin";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            System.out.println("==============正在请求数据库查询POI==============");
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= col; i++) {
                    String get = rs.getString(i);
                    if (get.length() == 20)
                        ret.add(rs.getString(i));
                }
            }
            System.out.println("===============完成请求数据库查询POI=============");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ret;
    }


    /*
    测试通过
    对于600W数据15s左右完成检索
    10788ms - 6061117
    27022ms,26777ms - 12041715
    41378ms - 20212532

    192328ms - 600w
     */
    public static int getKeyWordNum(String keyword) {
        String query = "SELECT distinct content FROM rawdata.checkin where content like \'%fuck%\'";
        Connection conn = getConn();
        PreparedStatement ps;
        int ret = 0;
        int checkNum = 0;
        long start = System.currentTimeMillis();
        try {
            ps = (PreparedStatement) conn.prepareStatement(query);
            System.out.println(ps.getPreparedSql());
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                checkNum ++;
                String content = rs.getString(1);
                String[] ress = content.split(keyword);
                ret += ress.length - 1;
                System.out.println(content + " \t " + checkNum);

            }
            System.out.println(checkNum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start + "ms 完成关键字检索");
        return ret;
    }

    public static int getKeyWordNum(String keyword, int LIMIT, int OFFSET) {
        String query = "SELECT distinct content FROM rawdata.checkin where content like \'%fuck%\' LIMIT "+OFFSET +" , " + LIMIT;
        Connection conn = getConn();
        PreparedStatement ps;
        int ret = 0;
        int checkNum = 0;
        long start = System.currentTimeMillis();
        try{
            ps = (PreparedStatement) conn.prepareStatement(query);
            System.out.println(ps.getPreparedSql());
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
                checkNum ++;
                String content = rs.getString(1);
                String[] ress = content.split(keyword);
                ret += ress.length - 1;
//                System.out.println(content + " \t " + checkNum);

            }
            System.out.println(checkNum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
//        System.out.println(end - start + "ms 完成关键字检索");
        return ret;
    }

    /**
     *  10788ms - 6061117
     *  27022ms,26777ms - 12041715
     *  41378ms,40889ms - 20212532
     * @param args
     * @throws SQLException
     */
    public static void main(String args[]) throws SQLException {
//        Connection con = getConn();
//       List<String> get = getAllPoiid();
//        System.out.println(get.size());
//       if (have("B2094450D56AA1FD429E",con)) {
//           System.out.println("right");
//        Logger log = Logger.getLogger("1024");
//        log.warning("hello 1024!!!");
//        }
        System.out.print(getKeyWordNum("原来",100000,1));
    }
}


