package MRCompetion.Output;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.concurrent.LinkedBlockingDeque;

import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/14.
 * 入口
 */
public class CheckInPutThread extends Thread {
    final static LinkedBlockingDeque<CheckIn> CIPTBQ = new LinkedBlockingDeque<>(50000);
    private volatile boolean shouldEnd = false;
    private static int num;
    private static int limit = 50000;
    private static volatile int offset = 0;
    public CheckInPutThread(int num) {
        CheckInPutThread.num = num;
    }
    public CheckInPutThread() {
        num = Integer.MAX_VALUE;
    }
    @Override
    public void run() {
        super.run();
        getAllPoiid();
    }
    private synchronized void addOffset() {
        offset = offset + limit;
    }
    private static String generateSql(int offset, int limit) {
        return "select DISTINCT * from rawdata.checkin LIMIT " +limit + " OFFSET " +offset ;
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
            offset = CheckInPutThread.offset;
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
                while (rs.next()) {
                    CIPTBQ.put(new CheckIn(rs.getString(1), rs.getString(7), rs.getString(2),rs.getString(3),
                            rs.getString(4),rs.getString(6),Integer.parseInt(rs.getString(5))));
                }
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
