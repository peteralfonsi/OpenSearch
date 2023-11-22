/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import org.opensearch.common.cache.tier.keystore.RBMIntKeyLookupStore;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AdHocRBMBenchmark {

    @State(Scope.Benchmark)
    public static class AddState {
        public RBMIntKeyLookupStore kls;
        @Param({"10000", "100000", "1000000", "10000000"})
        public int numAdds;
        Random rand;
        @Setup(Level.Invocation)
        public void setupAddState() {
            this.kls = new RBMIntKeyLookupStore(0L); // default modulo, no memory cap
            this.rand = Randomness.get();
        }
    }

    @State(Scope.Benchmark)
    public static class ContainsState {
        public RBMIntKeyLookupStore kls;
        @Param({"10000", "100000", "1000000", "10000000"})
        public int numAdds;
        public int[] values;
        Random rand;
        @Setup
        public void setupContainsState() {
            this.kls = new RBMIntKeyLookupStore(0L); // default modulo, no memory cap
            this.rand = Randomness.get();
            this.values = new int[numAdds];
            for (int i = 0; i < numAdds; i++) {
                int val = rand.nextInt();
                values[i] = val;
                kls.add(val);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class RemoveState {
        public RBMIntKeyLookupStore kls;
        @Param({"10000", "100000", "1000000", "10000000"})
        public int numAdds;
        public int[] values;
        Random rand;
        @Setup(Level.Invocation)
        public void setupRemoveState() {
            this.kls = new RBMIntKeyLookupStore(0L); // default modulo, no memory cap
            this.rand = Randomness.get();
            this.values = new int[numAdds];
            for (int i = 0; i < numAdds; i++) {
                int val = rand.nextInt();
                values[i] = val;
                kls.add(val);
            }
        }
    }

    // BENCHMARKS BELOW
    @Benchmark
    public void testAdd(AddState state) {
        for (int i = 0; i < state.numAdds; i++) {
            state.kls.add(state.rand.nextInt());
        }
    }

    @Benchmark
    public void testContains(ContainsState state, Blackhole bh) {
        for (int i = 0; i < state.numAdds; i++) {
            bh.consume(state.kls.contains(state.values[i]));
        }
    }

    @Benchmark
    public void testRemove(RemoveState state, Blackhole bh) {
        for (int i = 0; i < state.numAdds; i++) {
            bh.consume(state.kls.remove(state.values[i]));
        }
    }


}
