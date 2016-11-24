package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;

import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/3.
 * 我发誓这是我最后一个JDBC的项目
 * 5000row -> 0.04sec
 * 50000row -> 0.52sec
 * 500000row -> 6.12sec
 */
public class CheckInReadThread extends Thread{
    private volatile boolean shouldEnd = false;
    private static int num;
    private static int limit = 50000;
    private static int cache = 500000;
    private static volatile int offset = 0;
    public CheckInReadThread(int num) {
        CheckInReadThread.num = num;
    }
    public CheckInReadThread() {
        num = Integer.MAX_VALUE;
    }
    public final static LinkedBlockingDeque<CheckIn> cirBQ = new LinkedBlockingDeque<>(cache);
    @Override
    public void run(){
        super.run();
        getAllPoiid();
//        System.out.println("这是线程A");
    }

    /**
     * 调用这个生成阻塞队列
     */
    private void getAllPoiid() {
        Connection conn = getConn();
        PreparedStatement pstmt;
        int offset = 0;
        while(true) {
            if (shouldEnd) {
                break;
            }
            offset = CheckInReadThread.offset;
            addOffset();
            if (offset - limit > num) {
                shouldEnd = true;
            }
            try {
                String sql = generateSql(offset, limit);
                pstmt = (PreparedStatement) conn.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery();
                if (!rs.next()) { //没取到值 数据取完了
                    shouldEnd = true;
                }
                rs.beforeFirst(); //把游标移到最前
//                int col = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    cirBQ.put(new CheckIn(rs.getString(1), rs.getString(7), rs.getString(2),rs.getString(3),
                            rs.getString(4),rs.getString(6),Integer.parseInt(rs.getString(5))));
                }
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    private synchronized void addOffset() {
        offset = offset + limit + 1;
    }

    private static String generateSql(int offset, int limit) {
        return "select DISTINCT * from checkin LIMIT " +limit + " OFFSET " +offset ;
    }

    public static void main(String args[]) {

    }
}
