package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import dataRead.CheckIn;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static sql.jdbcConnector.getConn;
import static sql.jdbcConnector.have;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiGetThread extends Thread{
    public final static LinkedBlockingDeque<CheckIn> pgtBQ = new LinkedBlockingDeque<>(50000);
    @Override
    public void run()  {
        super.run();
        Connection con = getConn();
        int i = 0;
        while(true) {
            CheckIn c = null;
            try {
                c = cirBQ.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (c != null){
                try {
                    if (have(c.getPoiid(), con)) {
                        try {
                            pgtBQ.put(c);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                i = 0;
            } else {
                try {
//                    System.out.println(Thread.currentThread().getName() + ":检测到队列为空，暂停1秒");
                    Thread.sleep(1000);
                    i ++;
                    if (i > 100) {
                        throw new RunOutOfQueue();
                    }
                } catch (InterruptedException | RunOutOfQueue e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
