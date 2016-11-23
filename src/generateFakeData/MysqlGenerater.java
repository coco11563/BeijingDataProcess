package generateFakeData;

import MRCompetion.ThreadClass.CheckInReadThread;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import dataRead.CheckIn;
import org.jruby.RubyProcess;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static sql.jdbcConnector.*;

/**
 * Created by coco1 on 2016/11/21.
 */
public class MysqlGenerater {
    public static void main(String args[]) throws InterruptedException {
        CheckInReadThread cir = new CheckInReadThread();
        cir.start();
        for (int i = 0 ; i < 3; i ++) {
            insertionThread is = new insertionThread("2");
            is.start();
        }
        while (true) {
            Thread.sleep(10000);
            System.out.println(cirBQ.size());
        }
    }
    private static class insertionThread extends Thread{
        private final List<CheckIn> temp = new ArrayList<>();
        private String s;
        public insertionThread(String s) {
            this.s = s;
        }
        @Override
        public void run(){
            super.run();
            int i = 0;
            Connection conn = getConn();
            PreparedStatement ps = null;
            try {
                ps = (PreparedStatement) conn.prepareStatement(insertSql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    CheckIn c = cirBQ.take();
                    c.setIdstr(c.getIdstr() + s);
                    temp.add(c);
                    if (temp.size() > 10000 || i > 100) {
                        checkInInsert(temp, conn, ps);
                        temp.clear();
                        if (i > 100) {
                            System.out.println("ALL DONE");
                            ps.close();
                            conn.close();
                            this.interrupt();
                        }
                    }
                } catch (InterruptedException e) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    i ++;
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

    }

}
