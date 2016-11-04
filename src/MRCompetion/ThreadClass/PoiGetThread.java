package MRCompetion.ThreadClass;

import com.mysql.jdbc.Connection;
import dataRead.CheckIn;

import java.util.concurrent.LinkedBlockingDeque;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static sql.jdbcConnector.getConn;
import static sql.jdbcConnector.have;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiGetThread extends Thread{
    public final static LinkedBlockingDeque<CheckIn> pgtBQ = new LinkedBlockingDeque<>();
    @Override
    public void run() {
        Connection con = getConn();
        int i = 0;
        while(true) {
            CheckIn c = cirBQ.poll();
            if (c != null){
                if (have(c.getPoiid(), con)) {
                    pgtBQ.add(c);
                }
                i = 0;
            } else {
                if (i > 1000) {
                    System.out.println("当前线程暂停次数过多，执行释放");
                    Thread.currentThread().interrupt();
                    break;
                }
                try {
                    System.out.print(Thread.currentThread().getName() + ":检测到队列为空，暂停10秒");
                    Thread.sleep(10000);
                    i ++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
