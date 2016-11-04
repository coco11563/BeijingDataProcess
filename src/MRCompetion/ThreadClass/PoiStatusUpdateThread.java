package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;

import java.sql.SQLException;

import static MRCompetion.ThreadClass.PoiGetThread.pgtBQ;
import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiStatusUpdateThread extends Thread{
    public String generateSql(String field, int time) {
        return "update rawdata.poistatus_"+time + " with(rowlock) set checkinnum=checkinnum+1 where poiid=\'"+field + "\'";
    }
    @Override
    public void run() {
        Connection conn = getConn();
        PreparedStatement ps;
        String sql;
        while (true) {
            CheckIn c = pgtBQ.poll();
            if (c != null) {
                sql = generateSql(c.getPoiid(), c.getTime());
                try {
                    ps = (PreparedStatement) conn.prepareStatement(sql);
                    ps.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    System.out.println(Thread.currentThread().getName() + ":当前队列中无数据，Update线程暂停");
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void main(String args[]) {
        PoiStatusUpdateThread a = new PoiStatusUpdateThread();
        a.run();
    }
}
