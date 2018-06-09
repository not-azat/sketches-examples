package me.azat.sketches;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Generates elements on construction, and then streams them
 * (first `cardinality` of them) with uniform or zipf distribution.
 */
class StreamGenerator {
    private final long[] universe; // only this elements are streamed
    private final int maxCardinality;
    private final JDKRandomGenerator rnd;

    StreamGenerator(int seed, int maxCardinality) {
        this.maxCardinality = maxCardinality;
        this.rnd = new JDKRandomGenerator(seed);
        this.universe = rnd.longs(maxCardinality).toArray();
    }

    /**
     * Stream elements with zipf distribution
     *
     * @param cardinality how many elements of universe to stream
     * @param exponent zipf distribution parameter
     * @param length stream length
     */
    Stream<String> zipfStream(int cardinality, double exponent, long length) {
        Preconditions.checkArgument(
                cardinality >= 0 && cardinality <= maxCardinality,
                "0 <= cardinality <= maxCardinality");

        ZipfDistribution distribution = new ZipfDistribution(cardinality - 1, exponent);
        return IntStream
                .generate(distribution::sample)
                .mapToObj(x -> String.valueOf(universe[x]))
                .limit(length);
    }

    /**
     * Stream elements with uniform distribution
     *
     * @param cardinality how many elements of universe to stream
     * @param length stream length
     */
    Stream<String> uniformStream(int cardinality, long length) {
        Preconditions.checkArgument(
                cardinality >= 0 && cardinality <= maxCardinality,
                "0 <= cardinality <= maxCardinality");

        UniformIntegerDistribution distribution = new UniformIntegerDistribution(rnd, 0, cardinality - 1);
        return IntStream
                .generate(distribution::sample)
                .mapToObj(x -> String.valueOf(universe[x]))
                .limit(length);
    }
}
