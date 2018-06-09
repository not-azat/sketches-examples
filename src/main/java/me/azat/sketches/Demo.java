package me.azat.sketches;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.membership.BloomFilter;

/**
 * Created by Azat Abdulvaliev on 09/06/2018.
 */
public class Demo {

    static void bloom() {

        BloomFilter filter = new BloomFilter(1000, 0.01);

        filter.add("холодильник");
        filter.add("диван");

        filter.isPresent("холодильник"); // true
        filter.isPresent("шкаф"); // maybe

    }

    static void cms() {

        CountMinSketch sketch = new CountMinSketch(3, 100, 12345);

        sketch.add("холодильник", 2);
        sketch.add("диван", 1);

        sketch.estimateCount("диван"); // >= 1

    }

    static void hll() {

        HyperLogLog hll = new HyperLogLog(10);

        hll.offer("холодильник");
        hll.offer("диван");
        hll.offer("холодильник");

        hll.cardinality(); // около 2

    }
}
