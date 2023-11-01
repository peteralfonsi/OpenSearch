/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.time;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.indices.BytesReferenceSerializer;
import org.opensearch.indices.BytesReferenceSerializerOptionTwo;
import org.opensearch.indices.EhcacheSolutionOptionOne;
import org.opensearch.indices.EhcacheSolutionOptionTwo;
import org.opensearch.indices.IRCKeyWriteableSerializer;
import org.opensearch.indices.IndicesRequestCache;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
//@State(Scope.Benchmark)
public class AdHocSerializerBenchmark {

    public static boolean USE_MIXED = true;

    public static EhcacheSolutionOptionOne<Integer, BytesReference> getOptionOneCache() {
        EhcacheSolutionOptionOne.Builder<Integer, BytesReference> builder = new EhcacheSolutionOptionOne.Builder<>();
        builder.setMaximumWeightInBytes(10000000)
            .setKeyType(Integer.class)
            .setValueType(BytesReference.class)
            .setValueSerializer(new BytesReferenceSerializer())
            .setExpireAfterAccess(new TimeValue(1, TimeUnit.DAYS))
            .setStoragePath("/tmp/OptionOne")
            .setThreadPoolAlias("ehcacheTest")
            .setSettings(Settings.builder().build())
            .setIsEventListenerModeSync(true);
        return builder.build();
    }

    public static EhcacheSolutionOptionTwo<Integer, BytesReference> getOptionTwoCache() {
        EhcacheSolutionOptionTwo.Builder<Integer, BytesReference> builder = new EhcacheSolutionOptionTwo.Builder<>();
        builder.setMaximumWeightInBytes(10000000)
            .setKeyType(Integer.class)
            .setValueType(BytesReference.class)
            .setValueSerializer(new BytesReferenceSerializerOptionTwo())
            .setExpireAfterAccess(new TimeValue(1, TimeUnit.DAYS))
            .setStoragePath("/tmp/OptionTwo")
            .setThreadPoolAlias("ehcacheTest")
            .setSettings(Settings.builder().build())
            .setIsEventListenerModeSync(true);
        return builder.build();
    }

    public static EhcacheSolutionOptionOne<Integer, BytesReference> getOptionThreeCache() {
        // default java serialization for BytesReference
        // can reuse same class
        EhcacheSolutionOptionOne.Builder<Integer, BytesReference> builder = new EhcacheSolutionOptionOne.Builder<>();
        builder.setMaximumWeightInBytes(10000000)
            .setKeyType(Integer.class)
            .setValueType(BytesReference.class)
            .setExpireAfterAccess(new TimeValue(1, TimeUnit.DAYS))
            .setStoragePath("/tmp/OptionOne")
            .setThreadPoolAlias("ehcacheTest")
            .setSettings(Settings.builder().build())
            .setIsEventListenerModeSync(true);
        return builder.build();
    }

    public static BytesReference[] getBytesArrayValues(int iterations, int keySize) {
        // return an array of simple BytesArray for use in values
        byte[] valueContent = new byte[keySize];
        BytesReference[] values = new BytesReference[iterations];
        Random rand = Randomness.get();
        for (int i = 0; i < iterations; i++) {
            //rand.nextBytes(keyContent);
            rand.nextBytes(valueContent);
            //keys[i] = new BytesArray(keyContent);
            values[i] = new BytesArray(valueContent);
        }
        return values;
    }

    public static BytesReference[] getMixedValues(int iterations, int keySize) {

        BytesReference[] values = new BytesReference[iterations];
        Random rand = Randomness.get();
        for (int i = 0; i < iterations; i++) {
            if (rand.nextBoolean()) {
                byte[] valueContent = new byte[keySize];
                rand.nextBytes(valueContent);
                values[i] = new BytesArray(valueContent);
            } else {
                // add CompositeBytesReference
                byte[] valueContent1 = new byte[keySize / 2];
                byte[] valueContent2 = new byte[keySize - valueContent1.length];
                rand.nextBytes(valueContent1);
                rand.nextBytes(valueContent2);
                values[i] = CompositeBytesReference.of(new BytesArray(valueContent1), new BytesArray(valueContent2));
                assert values[i].getClass() == CompositeBytesReference.class;
            }

        }
        return values;
    }

    public static BytesReference[] getValues(int iterations, int keySize) {
        if (USE_MIXED) {
            return getMixedValues(iterations, keySize);
        }
        return getBytesArrayValues(iterations, keySize);
    }

    @State(Scope.Benchmark)
    public static class OptionOneState {
        public EhcacheSolutionOptionOne<Integer, BytesReference> optionOneTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionOne() {
            this.optionOneTier = getOptionOneCache();
            this.values = getValues(iterations, keySize);
            optionOneTier.setLatch(iterations);
            //System.out.println("setup, latch = " + optionOneTier.latch.getCount());
            // optionOneTier.close();
        }
    }

    @State(Scope.Benchmark)
    public static class OptionOneGetState {
        // Used for the get() benchmark
        public EhcacheSolutionOptionOne<Integer, BytesReference> optionOneTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionOneGet() {
            this.optionOneTier = getOptionOneCache();
            this.values = getValues(iterations, keySize);
            for (int i = 0; i < iterations; i++) {
                optionOneTier.put(i, values[i]);
            }

            // optionOneTier.setLatch(iterations); // no need for latch as get() is not async
            //System.out.println("setup, latch = " + optionOneTier.latch.getCount());

            // optionOneTier.close();
        }
    }

    @State(Scope.Benchmark)
    public static class OptionTwoState {
        public EhcacheSolutionOptionTwo<Integer, BytesReference> optionTwoTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionTwo() {
            this.optionTwoTier = getOptionTwoCache();
            this.values = getValues(iterations, keySize);
            optionTwoTier.setLatch(iterations);
            //System.out.println("setup, latch = " + optionOneTier.latch.getCount());

            // optionOneTier.close();
        }
    }

    @State(Scope.Benchmark)
    public static class OptionTwoGetState {
        // Used for the get() benchmark
        public EhcacheSolutionOptionTwo<Integer, BytesReference> optionTwoTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionOneGet() throws IOException {
            this.optionTwoTier = getOptionTwoCache();
            this.values = getValues(iterations, keySize);
            for (int i = 0; i < iterations; i++) {
                optionTwoTier.put(i, values[i]);
            }

            // optionOneTier.setLatch(iterations); // no need for latch as get() is not async
            //System.out.println("setup, latch = " + optionOneTier.latch.getCount());

            // optionOneTier.close();
        }
    }

    @State(Scope.Benchmark)
    public static class OptionThreeState {
        public EhcacheSolutionOptionOne<Integer, BytesReference> optionThreeTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionOne() {
            this.optionThreeTier = getOptionThreeCache();
            this.values = getValues(iterations, keySize);
            optionThreeTier.setLatch(iterations);
        }
    }

    @State(Scope.Benchmark)
    public static class OptionThreeGetState {
        // Used for the get() benchmark
        public EhcacheSolutionOptionOne<Integer, BytesReference> optionThreeTier;
        public BytesReference[] values;
        //@Param({"1", "100", "10000"})
        @Param({"1"})
        public int iterations;

        @Param({"1000", "10000", "100000"})
        public int keySize;
        @Setup
        public void setupOptionOneGet() {
            this.optionThreeTier = getOptionThreeCache();
            this.values = getValues(iterations, keySize);
            for (int i = 0; i < iterations; i++) {
                optionThreeTier.put(i, values[i]);
            }
        }
    }

    /// BENCHMARKS BELOW

    @Benchmark
    public void testOptionOnePut(OptionOneState state) throws Exception {
        // Test option one, where we pass hacky serializers to be used internally by ehcache.
        for (int i = 0; i < state.iterations; i++) {
            state.optionOneTier.put(i, state.values[i]);
        }
        state.optionOneTier.latch.await();
    }

    @Benchmark
    public void testOptionOneGet(OptionOneGetState state, Blackhole bh) throws Exception {
        for (int i = 0; i < state.iterations; i++) {
            bh.consume(state.optionOneTier.get(i));
        }
    }

    @Benchmark
    public void testOptionTwoPut(OptionTwoState state) throws Exception {
        for (int i = 0; i < state.iterations; i++) {
            state.optionTwoTier.put(i, state.values[i]);
        }
        state.optionTwoTier.latch.await();
    }

    @Benchmark
    public void testOptionTwoGet(OptionTwoGetState state, Blackhole bh) throws Exception {
        for (int i = 0; i < state.iterations; i++) {
            bh.consume(state.optionTwoTier.get(i));
        }
    }

    @Benchmark
    public void testOptionThreePut(OptionThreeState state) throws Exception {
        // Test option three, with default java serialization.
        for (int i = 0; i < state.iterations; i++) {
            state.optionThreeTier.put(i, state.values[i]);
        }
        state.optionThreeTier.latch.await();
    }

    @Benchmark
    public void testOptionThreeGet(OptionThreeGetState state, Blackhole bh) throws Exception {
        for (int i = 0; i < state.iterations; i++) {
            bh.consume(state.optionThreeTier.get(i));
        }
    }
}
