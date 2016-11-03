package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/3.
 * 我发誓这是我最后一个JDBC的项目
 */
public class CheckInReadThread extends Thread{
    final static LinkedBlockingDeque<CheckIn> cirBQ = new LinkedBlockingDeque<>();
    @Override
    public void run(){
        super.run();
        getAllPoiid();
        System.out.println("这是线程A");
    }
    /**
     * 调用这个生成阻塞队列
     */
    public static void getAllPoiid() {
        Connection conn = getConn();
        PreparedStatement pstmt;
        int limit = 500;
        int offset = 0;
        while(true) {
            try {
                String sql = generateSql(offset, limit);
                pstmt = (PreparedStatement) conn.prepareStatement(sql);
                System.out.println("==============正在请求数据库查询签到数据==============");
                ResultSet rs = pstmt.executeQuery();
                if (!rs.next()) { //没取到值
                    break;
                }
                rs.beforeFirst(); //把游标移到最前
                int col = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    cirBQ.add(new CheckIn(rs.getString(0), rs.getString(6), rs.getString(1),rs.getString(2),
                            rs.getString(3),rs.getString(5),Integer.parseInt(rs.getString(4))));
                }
                offset = offset + limit;
                System.out.println("===============完成请求数据库查询签到数据=============");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public static String generateSql(int offset, int limit) {
        return "select DISTINCT * from rawdata.checkin LIMIT " +limit + " OFFSET " +offset ;
    }
    public static void main(String args[]) {
        getAllPoiid();
    }
}
