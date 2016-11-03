package MRCompetion.StreamTest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Created by coco1 on 2016/10/31.
 */
public class StreamTest {
    public static void main(String args[]) throws IOException {
        long timesofiterator = 0;
        long timesofStream = 0;
        long timesofParallelStream = 0;
        long temptime1 = 0;
        long temptime2 = 0;
        String contents = new String(Files.readAllBytes(Paths.get("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\src\\MRCompetion\\StreamTestData\\testData")), StandardCharsets.UTF_8);
        List<String> words = Arrays.asList(contents.split("[\\P{L}]+"));
        temptime1 = System.currentTimeMillis();
        int count = 0;
        for (String w : words) {
            if (w.length() > 12) count ++;
        }
        temptime2 = System.currentTimeMillis();
        timesofiterator = temptime2 - temptime1;
        temptime1 = System.currentTimeMillis();
        long count2 = words.stream().filter(w -> w.length() > 12).count();
        temptime2 = System.currentTimeMillis();
        timesofStream = temptime2 - temptime1;
        temptime1 = System.currentTimeMillis();
        long count3 = words.parallelStream().filter(w -> w.length() > 12).count();
        temptime2 = System.currentTimeMillis();
        timesofParallelStream = temptime2 - temptime1;
        System.out.println("迭代器计算时间和结果是:" + count + " " + timesofiterator);
        System.out.println("Stream计算时间和结果是:" + count2 + " " + timesofStream);
        System.out.println("并行Stream计算时间和结果是:" + count3 + " " + timesofParallelStream);
        System.out.println("笔记本CPU数:"+Runtime.getRuntime().availableProcessors());
    }
}
