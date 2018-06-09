package me.azat.sketches;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.twitter.algebird.*;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Azat Abdulvaliev on 07/06/2018.
 */
public class CountMinSketchApp {


    public static void main(String[] args) {

        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);
        testEstimator(1_000, 5, 350);

        testEstimator(10_000, 4, 2000);
        testEstimator(10_000, 4, 2000);
        testEstimator(10_000, 4, 2000);
        testEstimator(10_000, 4, 2000);
        testEstimator(10_000, 4, 2000);

        testEstimator(100_000, 3, 9000);
        testEstimator(100_000, 3, 9000);
        testEstimator(100_000, 3, 9000);
        testEstimator(100_000, 3, 9000);

        testEstimator(1_000_000, 3, 32000);
        testEstimator(1_000_000, 3, 32000);
        testEstimator(1_000_000, 3, 32000);

        testEstimator(10_000_000, 3, 111000);
        testEstimator(10_000_000, 3, 111000);
        testEstimator(10_000_000, 3, 111000);
    }

    private static void testEstimator(int testCardinality, int depth, int width) {
        System.out.printf("--- Test with depth = %d, width = %d ---\n", depth, width);

        StreamGenerator gen = new StreamGenerator(testCardinality, testCardinality);

        HeavyHittersEstimator estimator = new StreamLibEstimator(depth, width, ThreadLocalRandom.current().nextInt());
//                HeavyHittersEstimator estimator = new AlgebirdEstimator();
        HashMapEstimator trueEstimator = new HashMapEstimator();

        // store
        gen.zipfStream(testCardinality, 0.5, testCardinality * 10)
                .forEach(e -> {
                    estimator.inc(e);
                    trueEstimator.inc(e);
                });

        // test

        String top1e = trueEstimator.topN(1).keySet().stream().findFirst().get();

        TObjectIntHashMap<String> top10 = trueEstimator.topN(10);
        double top10_sum_error = top10.keySet().stream()
                .mapToDouble(e -> Math.abs(trueEstimator.count(e) - estimator.count(e)))
                .sum()
                / top10.keySet().stream().mapToDouble(trueEstimator::count).sum();

        System.out.printf(
                "testCardinality: %d, trueCardinality: %d, top1_real: %d, top1_estimate: %d, top10_sum_error: %.2f%%, estimatorSize: %d, trueSize: %d\n",
                testCardinality,
                trueEstimator.map.size(),
                trueEstimator.count(top1e),
                estimator.count(top1e),
                100.0 * top10_sum_error,
                estimator.size(),
                trueEstimator.size());
    }


    interface HeavyHittersEstimator {
        void inc(String e);
        long count(String e);
        long size();
    }


    static class HashMapEstimator implements HeavyHittersEstimator {
        private final TObjectIntHashMap<String> map = new TObjectIntHashMap<>();

        @Override
        public void inc(String e) {
            map.adjustOrPutValue(e, 1, 1);
        }

        @Override
        public long count(String e) {
            return map.get(e);
        }

        public TObjectIntHashMap<String> topN(int n) {
            TObjectIntHashMap<String> result = new TObjectIntHashMap<>();
            map.keySet().stream()
                    .sorted(Comparator.comparing(map::get).reversed())
                    .limit(n)
                    .forEach(e -> result.put(e, map.get(e)));
            return result;
        }

        @Override
        public long size() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                map.writeExternal(new ObjectOutputStream(baos));
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    static class AlgebirdEstimator implements HeavyHittersEstimator {
        private final TopPctCMSMonoid<String> monoid;
        private TopCMS<String> cms;

        public AlgebirdEstimator() {
            this.monoid = TopPctCMS.monoid(0.001, 1e-10, 12345, 0.01, CMSHasher.CMSHasherString$.MODULE$);
            this.cms = monoid.zero();
        }

        @Override
        public void inc(String e) {
            cms = (TopCMS<String>)cms.$plus(e);
        }

        @Override
        public long count(String e) {
            return cms.frequency(e).numeric().toLong(cms.frequency(e).estimate());
        }

        @Override
        public long size() {
            try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos))
            {
                oos.writeObject(cms);
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    static class StreamLibEstimator implements HeavyHittersEstimator {
        private final CountMinSketch sketch;

        public StreamLibEstimator(double error, double confidence) {
            this.sketch = new CountMinSketch(error, confidence, 1);
        }

        public StreamLibEstimator(int depth, int width, int seed) {
            this.sketch = new CountMinSketch(depth, width, seed);
        }

        @Override
        public void inc(String e) {
            sketch.add(e, 1);
        }

        @Override
        public long count(String e) {
            return sketch.estimateCount(e);
        }

        @Override
        public long size() {
            return CountMinSketch.serialize(sketch).length;
        }
    }
}
