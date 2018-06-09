package me.azat.sketches;

import com.google.common.base.Stopwatch;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.twitter.algebird.Hash128$;
import gnu.trove.set.hash.THashSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generate some numerical strings, store 1%, 5%, 10%, 20%, or 50% of them in hash set and bloom filter.
 * Then test on 100% of them, and find false-positive rate.
 */
public class BloomFilterApp {
    public static void main(String[] args) {
        System.out.printf("--- Test with fpp = %.2f ---\n", 0.1);
        testEstimator(1_000_000, 0.1);

        System.out.printf("--- Test with fpp = %.2f ---\n", 0.05);
        testEstimator(1_000_000,0.05);

        System.out.printf("--- Test with fpp = %.2f ---\n", 0.03);
        testEstimator(1_000_000,0.03);

        System.out.printf("--- Test with fpp = %.2f ---\n", 0.01);
        testEstimator(1_000_000,0.01);
    }


    static void testEstimator(int maxCardinality, double falsePositiveProbability) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (int testCardinality = 1000; testCardinality <= maxCardinality; testCardinality *= 10) {
            StreamGenerator gen = new StreamGenerator(12345, testCardinality);

            for (double fillRate : new double[]{ 0.01, 0.05, 0.1, 0.2, 0.5 }) {
                int storeCardinality = (int)(testCardinality * fillRate); // store fillRate of all elements
                int streamLength = testCardinality * 5;

//                MembershipEstimator estimator = new GuavaBloomEstimator(storeCardinality, falsePositiveProbability);
                MembershipEstimator estimator = new StreamLibBloomEstimator(storeCardinality, falsePositiveProbability);
//                MembershipEstimator estimator = new AlgebirdBloomEstimator(storeCardinality, falsePositiveProbability);

                MembershipEstimator trueEstimator = new HashSetEstimator();

                // store
                gen.uniformStream(storeCardinality, streamLength)
                        .forEach(e -> {
                            estimator.add(e);
                            trueEstimator.add(e);
                        });

                // test
                AtomicInteger truePositives = new AtomicInteger();
                AtomicInteger falsePositives = new AtomicInteger();
                AtomicInteger total = new AtomicInteger();
                gen.uniformStream(testCardinality, streamLength)
                        .forEach(e -> {
                            boolean isTrue = trueEstimator.contains(e);
                            boolean positive = estimator.contains(e);
                            if (isTrue && positive) {
                                truePositives.incrementAndGet();
                            }
                            if (!isTrue && positive) {
                                falsePositives.incrementAndGet();
                            }
                            total.incrementAndGet();
                            if (isTrue && !positive) {
                                throw new AssertionError();
                            }
                        });

                System.out.printf(
                        "filled: %d%%, testCardinality: %d, tests: %d, truePositives: %d, falsePositives: %d, fpRate: %.2f%%, estimatorSize: %d, trueSize: %d\n",
                        (int)(fillRate * 100.0),
                        testCardinality,
                        total.get(),
                        truePositives.get(),
                        falsePositives.get(),
                        100.0 * falsePositives.get() / total.get(),
                        estimator.size(),
                        trueEstimator.size());
            }
        }
        System.out.printf("Finished in %s\n", stopwatch);
    }


    interface MembershipEstimator {
        void add(String e);
        boolean contains(String e);
        long size();
    }


    static class HashSetEstimator implements MembershipEstimator {
        private final THashSet<String> elements;

        public HashSetEstimator() {
            this.elements = new THashSet<>();
        }

        @Override
        public void add(String e) {
            elements.add(e);
        }

        @Override
        public boolean contains(String e) {
            return elements.contains(e);
        }

        @Override
        public long size() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                elements.writeExternal(new ObjectOutputStream(baos));
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    static class GuavaBloomEstimator implements MembershipEstimator {
        private final BloomFilter<String> bloomFilter;

        GuavaBloomEstimator(int expectedInsertions, double fpp) {
            this.bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(Charset.defaultCharset()),
                    expectedInsertions,
                    fpp);
        }

        @Override
        public void add(String e) {
            bloomFilter.put(e);
        }

        @Override
        public boolean contains(String e) {
            return bloomFilter.mightContain(e);
        }

        @Override
        public long size() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                bloomFilter.writeTo(baos);
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


    static class StreamLibBloomEstimator implements MembershipEstimator {
        private final com.clearspring.analytics.stream.membership.BloomFilter filter;

        StreamLibBloomEstimator(int expectedInsertions, double fpp) {
            this.filter = new com.clearspring.analytics.stream.membership.BloomFilter(expectedInsertions, fpp);
        }

        @Override
        public void add(String e) {
            filter.add(e);
        }

        @Override
        public boolean contains(String e) {
            return filter.isPresent(e);
        }

        @Override
        public long size() {
            return com.clearspring.analytics.stream.membership.BloomFilter.serialize(filter).length;
        }
    }


    static class AlgebirdBloomEstimator implements MembershipEstimator {
        private com.twitter.algebird.BF<String> bloomFilter;

        AlgebirdBloomEstimator(int expectedInsertions, double fpp) {
            this.bloomFilter = com.twitter.algebird.BloomFilter
                    .apply(expectedInsertions, fpp, Hash128$.MODULE$.murmur128Utf8String(12345))
                    .zero();
        }

        @Override
        public void add(String e) {
            bloomFilter = bloomFilter.$plus(e); // why so heavy implementation?
        }

        @Override
        public boolean contains(String e) {
            return bloomFilter.maybeContains(e);
        }

        @Override
        public long size() {
            try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos))
            {
                oos.writeObject(bloomFilter);
                return baos.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }


}
