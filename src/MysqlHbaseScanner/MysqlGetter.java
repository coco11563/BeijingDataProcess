package MysqlHbaseScanner;

import MRCompetion.ThreadClass.CheckInReadThread;
import dataRead.CheckIn;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by coco1 on 2016/11/18.
 * extends from CheckInReadThread
 * 这个类实现了一个线程
 * 不断从MYSQL中提取Checkin
 * 然后写入到这个类中的阻塞队列
 * 同时从这个类中实现一个函数
 * 对Checkin数据进行筛选
 */
public class MysqlGetter extends CheckInReadThread {

    public class MysqlScanner extends Thread{
        @Override
        public void run(){

        }

        public int mysqlScanner(){
            return 0;
        }
    }
}
