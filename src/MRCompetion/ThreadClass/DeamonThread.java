package MRCompetion.ThreadClass;

import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static MRCompetion.ThreadClass.PoiGetThread.pgtBQ;

/**
 * Created by coco1 on 2016/11/6.
 * 用以统计总体运行时间
 */
public class DeamonThread extends Thread {
    static long time_start;
    static long time_end;
    static int n = 0;
    @Override
    public void run() {
        super.run();
        time_start = System.currentTimeMillis();
        int len_cirb;
        int len_pgtb;
        int len_cira;
        int len_pgta;

        while(true) {
            len_cirb = cirBQ.size();
            len_pgtb = pgtBQ.size();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            len_cira = cirBQ.size();
            len_pgta = pgtBQ.size();
            System.out.println(len_cirb + "," + len_pgtb + "," + len_cira + "," + len_pgta);
            if (len_cira == len_cirb && len_cira == 0 && len_pgta == len_pgtb && len_pgta == 0) {
                n++;
                if (n > 100) {
                    time_end = System.currentTimeMillis();
                    System.out.println("这一次执行时间为：" + (time_end - time_start) + "ms");
                    break;
                }
            } else {
                n = 0;
            }

        }
    }
}
