package MRCompetion.Output;

import MRCompetion.ThreadClass.CheckInReadThread;

import static MRCompetion.Competion.printThreadList;

/**
 * Created by coco1 on 2016/11/14.
 */
public class main {
    public static void main(String args[]) throws InterruptedException {
        CheckInPutThread checkInPutThread = new CheckInPutThread(600000); //1
        checkInPutThread.start();
        CheckOutPutThread checkOutPutThread; //N
        for (int i = 0 ; i < 8 ; i ++) {
            checkOutPutThread = new CheckOutPutThread();
            checkOutPutThread.start();
        }
        OutPutThread outPutThread = new OutPutThread(); //1
        outPutThread.start();
        DeamonThread deamonThread = new DeamonThread();
        deamonThread.start();
        while (true) {
            Thread.sleep(10000);
            printThreadList();
        }
    }
}
