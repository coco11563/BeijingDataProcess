package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import dataRead.CheckIn;

import java.sql.SQLException;

import static MRCompetion.ThreadClass.PoiGetThread.pgtBQ;
import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/3.
 *
 * 这个线程用于从CIRT中生成的阻塞队列中不断地取出数据并进行更新数据库
 *
 */
public class PoiStatusUpdateThread extends Thread{
    public volatile boolean shouldEnd = false;
    private String generateSql(String field, int time) {
//        System.out.println("更正：" + field + "|时间为：" + time);
        return "update rawdata.poistatus_"+time + " set checkinnum=checkinnum+1 where poiid=\'"+field + "\'";
    }
    private String generateSqlLock(int time) {
        return "lock tables rawdata.poistatus_" + time + " write" ;
    }
    private String generateSqlUnLock() {
        return "unlock tables";
    }
    @Override
    public void run() {
        super.run();
        Connection conn = getConn();
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement stmt = (Statement) conn.createStatement();
//            PreparedStatement ps;
//            PreparedStatement ps_lock;
//            PreparedStatement ps_unlock;
            String sql_unlock ;
            String sql;
            String sql_lock;
            int addnum = 0;
            int n = 0;
            CheckIn c;
        while (true) {
            if (shouldEnd) {
                break;
            }
            try {
                c = pgtBQ.take();
            } catch (InterruptedException e) {
                n ++;
                if (n > 100) {
                    shouldEnd = true; //终止线程
                }
                e.printStackTrace();
                continue;
            }
            if (c != null) {
                addnum ++;
                sql = generateSql(c.getPoiid(), c.getTime());
                sql_lock = generateSqlLock(c.getTime());
                sql_unlock = generateSqlUnLock();
                stmt.addBatch(sql_lock);
                stmt.addBatch(sql);
                stmt.addBatch(sql_unlock);
                if (addnum % 3000 == 0 || pgtBQ.size() == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    conn.commit();
                }
//                    ps = (PreparedStatement) conn.prepareStatement(sql);
//                    ps_lock = (PreparedStatement) conn.prepareStatement(sql_lock);
//                    ps_unlock = (PreparedStatement) conn.prepareStatement(sql_unlock);
//                    ps_lock.execute();
//                    ps.execute();
//                    ps_unlock.execute();
            } else {
                try {
//                    System.out.println(Thread.currentThread().getName() + ":当前队列中无数据，Update线程暂停");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void main(String args[]) {
        PoiStatusUpdateThread a = new PoiStatusUpdateThread();
        a.run();
    }
}
