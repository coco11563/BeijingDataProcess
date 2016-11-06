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
        System.out.println("更正：" + field + "|时间为：" + time);
        return "update rawdata.poistatus_"+time + " set checkinnum=checkinnum+1 where poiid=\'"+field + "\'";
    }
    public String generateSqlLock(int time) {
        return "lock tables rawdata.poistatus_" + time + " write" ;
    }
    @Override
    public void run() {
        Connection conn = getConn();
        PreparedStatement ps;
        PreparedStatement ps_lock;
        String sql_unlock = "unlock tables";
        PreparedStatement ps_unlock;
        String sql;
        String sql_lock;
        while (true) {
            CheckIn c = pgtBQ.poll();
            if (c != null) {
                sql = generateSql(c.getPoiid(), c.getTime());
                sql_lock = generateSqlLock(c.getTime());
                try {
                    ps = (PreparedStatement) conn.prepareStatement(sql);
                    ps_lock = (PreparedStatement) conn.prepareStatement(sql_lock);
                    ps_unlock = (PreparedStatement) conn.prepareStatement(sql_unlock);
                    ps_lock.execute();
                    ps.execute();
                    ps_unlock.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    System.out.println(Thread.currentThread().getName() + ":当前队列中无数据，Update线程暂停");
                    Thread.sleep(1000);
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
