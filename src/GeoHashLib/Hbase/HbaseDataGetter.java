package GeoHashLib.Hbase;

import java.util.concurrent.Callable;

/**
 * Created by root on 12/9/16.
 */
public class HbaseDataGetter implements Callable<String> {

    public static void getterBatcher(int num) {

    }

    @Override
    public String call() throws Exception {
        return null;
    }
    public static void main(String args[]) throws Exception {
        HbaseDataGetter hdg = new HbaseDataGetter();
        hdg.call();
    }
}
