package MRCompetion.Output;

import static MRCompetion.Output.CheckInPutThread.CIPTBQ;
import static MRCompetion.Output.CheckOutPutThread.COPTBQ;
import static MRCompetion.ThreadClass.CheckInReadThread.cirBQ;
import static MRCompetion.ThreadClass.PoiGetThread.pgtBQ;

/**
 * Created by coco1 on 2016/11/6.
 * 用以统计总体运行时间
 */
public class DeamonThread extends Thread {
    private static int n = 0;
    @Override
    public void run() {
        super.run();
        long time_start = System.currentTimeMillis();
        int len_ciptbqa;
        int len_ciptbqb;
        int len_coptbqa;
        int len_coptbqb;

        while(true) {
            len_ciptbqa = CIPTBQ.size();
            len_coptbqa = COPTBQ.size();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            len_ciptbqb = CIPTBQ.size();
            len_coptbqb = COPTBQ.size();
            System.out.println(len_ciptbqa + "," + len_coptbqa + "," + len_ciptbqb + "," + len_coptbqb);
            if (len_ciptbqa == len_ciptbqb && len_coptbqa == 0 && len_coptbqa == len_coptbqb && len_coptbqb == 0) {
                n++;
                if (n > 100) {
                    long time_end = System.currentTimeMillis();
                    System.out.println("执行时间为：" + (time_end - time_start) + "ms");
                    break;
                }
            } else {
                n = 0;
            }

        }
    }
}
