package MRCompetion;

import MRCompetion.ThreadClass.CheckInReadThread;
import MRCompetion.ThreadClass.DeamonThread;
import MRCompetion.ThreadClass.PoiGetThread;
import MRCompetion.ThreadClass.PoiStatusUpdateThread;

/**
 * Created by coco1 on 2016/10/31.
 * 北京市共606万条数据
 * 实验分为10个量级
 * 从60W -> 606W
 * 6W:352797ms
 * 6W:298663ms
 * 6W:124992ms
 * 60W:1220218ms
 */
public class Competion {
    static int num = 1200000;
    public static void main(String args[]) {
        generateThread(num);
    }
    /**
     * 生成进程
     */
    private static void generateThread(int num) {
        DeamonThread dt = new DeamonThread();
        dt.start();
        System.out.println("正在进行读取线程初始化...");
        CheckInReadThread c = new CheckInReadThread(num);
        c.start();
        System.out.println("正在进行查询线程初始化...");
        for (int i = 0 ; i < 4 ; i ++) {
            PoiGetThread pgt = new PoiGetThread();
            pgt.start();
        }
        System.out.println("正在进行更新线程初始化...");
        for (int i = 0 ; i < 4 ; i ++) {
            PoiStatusUpdateThread psut = new PoiStatusUpdateThread();
            psut.start();
        }
    }
    /**
     * 进程情况打印
     */
    private static void printThreadList() {
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
