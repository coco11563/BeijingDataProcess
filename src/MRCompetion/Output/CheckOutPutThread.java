package MRCompetion.Output;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;

import static MRCompetion.Output.CheckInPutThread.CIPTBQ;
import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/11.
 *
 * 使用这个工程将checkin中所有要求的数据导出
 *
 * 使用这个项目将所有的poiinform数据导出
 */
public class CheckOutPutThread extends Thread{
    final static LinkedBlockingDeque<IntoMRCheckin> COPTBQ = new LinkedBlockingDeque<>(50000);
    @Override
    public void run() {
        Connection conn = getConn();
        int i = 0;
        CheckIn c;
        String type = null;
        while(true) {
            try {
                if(i > 100) { //100次以上连续空则停止这个进程
                    Thread.sleep(100);
                    break;
                }
                c = CIPTBQ.take();
                i = 0;
                type = have(c,conn);
                COPTBQ.put(new IntoMRCheckin(c,type));
            } catch (InterruptedException e) {
                i ++;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
    /**
     * @param checkIn 传入poiid讯息
     * @param connection 使用全局变量存储连接讯息
     * @return 存在则返回type，不存在则返回null
     */
    private static String have(CheckIn checkIn, Connection connection) throws SQLException {
        PreparedStatement ps = null;
        String type = null;
        String sql = "select a.type from rawdata.poiinform a WHERE a.poiid=\'" + checkIn.getPoiid()+"\'LIMIT 1";
        try {
            ps = (PreparedStatement)connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) { //需要把游标移动到第一位
                type = rs.getString(1);
            }
            ps.close();
        } catch (SQLException e) {
            System.err.print("Wrong while query the num");
            e.printStackTrace();
        }
        return type;
    }
}
