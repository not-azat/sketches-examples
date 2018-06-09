package me.azat.sketches;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Stopwatch;
import com.twitter.algebird.HLL;
import com.twitter.algebird.Hash128$;
import com.twitter.algebird.HyperLogLogMonoid;
import gnu.trove.set.hash.THashSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * Created by Azat Abdulvaliev on 07/06/2018.
 */
public class HyperLogLogApp {

    public static void main(String[] args) {
        System.out.printf("--- Test with error = %.2f ---\n", 0.1);
        testEstimator(1_000_000, 0.1);

        System.out.printf("--- Test with error = %.2f ---\n", 0.05);
        testEstimator(1_000_000, 0.05);

        System.out.printf("--- Test with error = %.2f ---\n", 0.01);
        testEstimator(1_000_000, 0.01);
    }

    private static void testEstimator(int maxCardinality, double error) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (int testCardinality = 1000; testCardinality <= maxCardinality; testCardinality *= 10) {
            for (int i = 0; i < 5; i++) {
                StreamGenerator gen = new StreamGenerator(i, testCardinality);

                CardinalityEstimator estimator = new StreamLibEstimator(error);
//                CardinalityEstimator estimator = new AlgebirdEstimator(error);
                CardinalityEstimator trueEstimator = new HashSetEstimator();

                // store
                gen.uniformStream(testCardinality, testCardinality * 10)
                        .forEach(e -> {
                            estimator.add(e);
                            trueEstimator.add(e);
                        });

                // test
                double trueCardinality = trueEstimator.cardinality();
                double estimatedCardinality = estimator.cardinality();
                double statError = Math.abs(estimatedCardinality - trueCardinality) / trueCardinality;

                System.out.printf(
                        "streamCardinality: %d, trueCardinality: %.1f, estimatedCardinality: %.1f, error: %.2f%%, estimatorSize: %d, trueSize: %d\n",
                        testCardinality,
                        trueCardinality,
                        estimatedCardinality,
                        100 * statError,
                        estimator.size(),
                        trueEstimator.size());
            }
        }
        System.out.printf("Finished in %s\n", stopwatch);
    }


    interface CardinalityEstimator {
        void add(String e);
        double cardinality();
        int size();
    }


    static class HashSetEstimator implements CardinalityEstimator {
        private final THashSet<String> elements = new THashSet<>();

        @Override
        public void add(String e) {
            elements.add(e);
        }

        @Override
        public double cardinality() {
            return elements.size();
        }

        @Override
        public int size() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                elements.writeExternal(new ObjectOutputStream(baos));
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    static class StreamLibEstimator implements CardinalityEstimator {
        private final HyperLogLog hll;

        public StreamLibEstimator(double error) {
            int log2m = (int)Math.ceil(2.0 * Math.log(1.04 / error) / Math.log(2.0));
            this.hll = new HyperLogLog(log2m);
        }

        @Override
        public void add(String e) {
            hll.offer(e);
        }

        @Override
        public double cardinality() {
            return hll.cardinality();
        }

        @Override
        public int size() {
            return hll.sizeof();
        }
    }


    static class AlgebirdEstimator implements CardinalityEstimator {
        private final HyperLogLogMonoid monoid;
        private HLL hll;

        public AlgebirdEstimator(double error) {
            this.monoid = new HyperLogLogMonoid(com.twitter.algebird.HyperLogLog.bitsForError(error));
            this.hll = monoid.zero();
        }

        @Override
        public void add(String e) {
            hll = hll.$plus(monoid
                    .toHLL(e, Hash128$.MODULE$.murmur128Utf8String(12345))
                    .toDenseHLL());
        }

        @Override
        public double cardinality() {
            return monoid.estimateSize(hll);
        }

        @Override
        public int size() {
            return hll.size();
        }
    }
}
