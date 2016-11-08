package Test;

/**
 * Created by coco1 on 2016/11/7.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
public class TestMuTable {
    public static void main(String args[]) {
    String[] names = {"peter", "paul", "mary"};
    List<Runnable> runners = new ArrayList<>();
    for(int i = 0 ; i < names.length ; i ++) {
        String name = names[i];
        runners.add(()->
            System.out.println(name)
        );
    }
    for (Runnable runner : runners) {
        runner.run();
    }
}
}