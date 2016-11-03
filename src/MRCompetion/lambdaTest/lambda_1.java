package MRCompetion.lambdaTest;

import sun.plugin.javascript.navig.Array;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by coco1 on 2016/11/1.
 */
public class lambda_1 {

    public static int compare(String a, String b) {
        Comparator<String> stringComparator = ( first,  second) -> Integer.compare(first.length(), second.length());
        return stringComparator.compare(a,b);
    }
    public static void main(String args[]) {
        System.out.println(compare("123","1"));
    }
}
