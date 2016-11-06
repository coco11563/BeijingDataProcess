package MRCompetion;

import MRCompetion.ThreadClass.CheckInReadThread;
import MRCompetion.ThreadClass.PoiGetThread;
import MRCompetion.ThreadClass.PoiStatusUpdateThread;

/**
 * Created by coco1 on 2016/10/31.
 */
public class Competion {
    public static void main(String args[]) {
        System.out.println("正在进行读取线程初始化...");
        CheckInReadThread c = new CheckInReadThread();
        c.start();
        System.out.println("正在进行查询线程初始化...");
        for (int i = 0 ; i < 4 ; i ++) {
            PoiGetThread pgt = new PoiGetThread();
            pgt.start();
        }
        System.out.println("正在进行更新线程初始化...");
        for (int i = 0 ; i < 10 ; i ++) {
            PoiStatusUpdateThread psut = new PoiStatusUpdateThread();
            psut.start();
        }
        while(true) {
            try {
                Thread.sleep(10000);
                printThreadList();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public static void printThreadList() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while (group.getParent() != null) {
            group = group.getParent();
        }
        Thread[] threads = new Thread[group.activeCount()];
        group.enumerate(threads);
        for (Thread thread : threads) {
            if (thread == null) {
                continue;
            }
            try {
                StringBuffer buf = new StringBuffer();
                ThreadGroup tgroup = thread.getThreadGroup();
                String groupName = tgroup == null ? "null" : tgroup.getName();
                buf.append("ThreadGroup:").append(groupName).append(", ");
                buf.append("Id:").append(thread.getId()).append(", ");
                buf.append("Name:").append(thread.getName()).append(", ");
                buf.append("isDaemon:").append(thread.isDaemon()).append(", ");
                buf.append("isAlive:").append(thread.isAlive()).append(", ");
                buf.append("Priority:").append(thread.getPriority());
                System.out.println(buf.toString());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }
}
