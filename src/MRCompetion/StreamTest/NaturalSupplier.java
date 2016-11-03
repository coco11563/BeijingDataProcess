package MRCompetion.StreamTest;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Created by coco1 on 2016/11/1.
 */
public class NaturalSupplier implements Supplier<Long> {
    long st = 0;
    @Override
    public Long get() {
        this.st = this.st + 1;
        return this.st;
    }
    public static void main(String args[]) {
        Stream<Long> natural = Stream.generate(new NaturalSupplier());
        natural.map((x) -> {
            return x * x;
        }).limit(10).forEach(System.out::println);
    }
}
