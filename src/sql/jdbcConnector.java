package sql;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by coco1 on 2016/10/19.
 */
public class jdbcConnector {
    private static final String DATABASEADDRESS = "jdbc:mysql://localhost:3306/rawdata";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String insertSql  = "insert into checkin (checkin,lat,lng,poiid,clock,content,datetime) values(?,?,?,?,?,?,?)";


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
        String sql = "insert into checkin (checkin,lat,lng,poiid,clock,content) values(?,?,?,?,?,?)";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            pstmt.setString(1, checkIn.getIdstr());
            pstmt.setString(2, checkIn.getLat());
            pstmt.setString(3, checkIn.getLng());
            pstmt.setString(4, checkIn.getPoiid());
            pstmt.setInt(5, checkIn.getTime());
            pstmt.setString(6, checkIn.getContent());
            i = pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
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
        pstmt.executeBatch();
        connection.commit();
    }

    private static Integer getAll() {
        Connection conn = getConn();
        String sql = "select * from rawdata.checkin";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            System.out.println("============================");
            while (rs.next()) {
                for (int i = 1; i <= col; i++) {
                    System.out.print(rs.getString(i) + "\t");
                    if ((i == 2) && (rs.getString(i).length() < 8)) {
                        System.out.print("\t");
                    }
                }
                System.out.println("");
            }
            System.out.println("============================");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}


