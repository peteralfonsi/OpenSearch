/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.Numbers;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongLongHash;
import org.opensearch.common.util.ReorganizingLongHash;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.search.aggregations.CardinalityUpperBound;

/**
 * Maps long bucket keys to bucket ordinals.
 *
 * @opensearch.internal
 */
public abstract class LongKeyedBucketOrds implements Releasable {
    /**
     * Build a {@link LongKeyedBucketOrds}.
     */
    public static LongKeyedBucketOrds build(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        return cardinality.map(estimate -> estimate < 2 ? new FromSingle(bigArrays) : new FromMany(bigArrays));
    }

    private LongKeyedBucketOrds() {}

    /**
     * Add the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    public abstract long add(long owningBucketOrd, long value);

    /**
     * Count the buckets in {@code owningBucketOrd}.
     * <p>
     * Some aggregations expect this to be fast but most wouldn't
     * mind particularly if it weren't.
     */
    public abstract long bucketsInOrd(long owningBucketOrd);

    /**
     * Find the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have been added or {@code -1} if they haven't.
     */
    public abstract long find(long owningBucketOrd, long value);

    /**
     * Returns the value currently associated with the bucket ordinal
     */
    public abstract long get(long ordinal);

    /**
     * The number of collected buckets.
     */
    public abstract long size();

    /**
     * The maximum possible used {@code owningBucketOrd}.
     */
    public abstract long maxOwningBucketOrd();

    /**
     * Build an iterator for buckets inside {@code owningBucketOrd} in order
     * of increasing ord.
     * <p>
     * When this is first returns it is "unpositioned" and you must call
     * {@link BucketOrdsEnum#next()} to move it to the first value.
     */
    public abstract BucketOrdsEnum ordsEnum(long owningBucketOrd);

    /**
     * An iterator for buckets inside a particular {@code owningBucketOrd}.
     *
     * @opensearch.internal
     */
    public interface BucketOrdsEnum {
        /**
         * Advance to the next value.
         * @return {@code true} if there *is* a next value,
         *         {@code false} if there isn't
         */
        boolean next();

        /**
         * The ordinal of the current value.
         */
        long ord();

        /**
         * The current value.
         */
        long value();

        /**
         * An {@linkplain BucketOrdsEnum} that is empty.
         */
        BucketOrdsEnum EMPTY = new BucketOrdsEnum() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public long ord() {
                return 0;
            }

            @Override
            public long value() {
                return 0;
            }
        };
    }

    /**
     * Implementation that only works if it is collecting from a single bucket.
     *
     * @opensearch.internal
     */
    public static class FromSingle extends LongKeyedBucketOrds {
        private final ReorganizingLongHash ords;

        public FromSingle(BigArrays bigArrays) {
            ords = new ReorganizingLongHash(bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            assert owningBucketOrd == 0;
            return ords.add(value);
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            assert owningBucketOrd == 0;
            return ords.find(value);
        }

        @Override
        public long get(long ordinal) {
            return ords.get(ordinal);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return ords.size();
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            return 0;
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return new BucketOrdsEnum() {
                private long ord = -1;
                private long value;

                @Override
                public boolean next() {
                    ord++;
                    if (ord >= ords.size()) {
                        return false;
                    }
                    value = ords.get(ord);
                    return true;
                }

                @Override
                public long value() {
                    return value;
                }

                @Override
                public long ord() {
                    return ord;
                }
            };
        }

        @Override
        public void close() {
            ords.close();
        }
    }

    /**
     * Implementation that works properly when collecting from many buckets.
     *
     * @opensearch.internal
     */
    public static class FromMany extends LongKeyedBucketOrds {
        private final LongLongHash ords;

        public FromMany(BigArrays bigArrays) {
            ords = new LongLongHash(2, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            return ords.add(owningBucketOrd, value);
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            return ords.find(owningBucketOrd, value);
        }

        @Override
        public long get(long ordinal) {
            return ords.getKey2(ordinal);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            // TODO it'd be faster to count the number of buckets in a list of these ords rather than one at a time
            long count = 0;
            for (long i = 0; i < ords.size(); i++) {
                if (ords.getKey1(i) == owningBucketOrd) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            // TODO this is fairly expensive to compute. Can we avoid needing it?
            long max = -1;
            for (long i = 0; i < ords.size(); i++) {
                max = Math.max(max, ords.getKey1(i));
            }
            return max;
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            // TODO it'd be faster to iterate many ords at once rather than one at a time
            return new BucketOrdsEnum() {
                private long ord = -1;
                private long value;

                @Override
                public boolean next() {
                    while (true) {
                        ord++;
                        if (ord >= ords.size()) {
                            return false;
                        }
                        if (ords.getKey1(ord) == owningBucketOrd) {
                            value = ords.getKey2(ord);
                            return true;
                        }
                    }
                }

                @Override
                public long value() {
                    return value;
                }

                @Override
                public long ord() {
                    return ord;
                }
            };
        }

        @Override
        public void close() {
            ords.close();
        }
    }

    /**
     * Implementation which can skip using a hash table for histogram aggregations, since it knows the minimum value it might see.
     * Assumes incoming values will be densely packed. For example if the minimum key is 5, it expects to see values like 5, 6, 7, ... which become keys like 0, 1, 2, ...
     * Can only be used if it is collecting from a single bucket.
     */
    public static class MinimumAwareBucketOrds extends LongKeyedBucketOrds {
        private final long minimumValue;
        private long largestKeySeen;
        private final BigArrays bigArrays;
        private static final long MAX_CAPACITY = 1L << 32;
        // pkg-private for testing
        static final long DEFAULT_INITIAL_CAPACITY = 32;
        private long capacity;

        // pkg-private for testing
        IntArray alreadySeen; // While IntArray takes a bit more space than ByteArray, it seems significantly faster in benchmarks

        public MinimumAwareBucketOrds(long minimumValue, BigArrays bigArrays) {
            this.minimumValue = minimumValue;
            this.largestKeySeen = 0;
            this.bigArrays = bigArrays;
            this.capacity = DEFAULT_INITIAL_CAPACITY;

            try {
                alreadySeen = bigArrays.newIntArray(capacity, true);
                alreadySeen.fill(0, capacity, 0);  // 0 represents not yet seen
            } finally {
                if (alreadySeen == null) {
                    Releasables.closeWhileHandlingException(alreadySeen);
                }
            }
        }

        // Sets that we've now seen this key, and return 0 if we hadn't seen it before and 1 if we had. Grow array if needed.
        private int setAlreadySeen(long key) {
            if (alreadySeen.size() < key + 1) {
                assert key < MAX_CAPACITY : "incoming key is larger than the max capacity";
                capacity = Numbers.nextPowerOfTwo(key + 1);
                alreadySeen = bigArrays.resize(alreadySeen, capacity);
            }
            return alreadySeen.set(key, 1);
        }

        private long valueToKey(long value) {
            assert value >= minimumValue;
            return value - minimumValue;
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            assert owningBucketOrd == 0;
            long key = valueToKey(value);
            if (key > largestKeySeen) {
                largestKeySeen = key;
            }
            int seen = setAlreadySeen(key);
            // seen == 0 --> not yet seen this key
            if (seen == 0) {
                return key;
            }
            return -1 - key;
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            assert owningBucketOrd == 0;
            if (alreadySeen.get(valueToKey(value)) == 0) {
                return -1;
            }
            return valueToKey(value);
        }

        @Override
        public long get(long ordinal) {
            return valueToKey(ordinal);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return largestKeySeen + 1;
        }

        @Override
        public long size() {
            return largestKeySeen + 1;
        }

        @Override
        public long maxOwningBucketOrd() {
            return 0;
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return new BucketOrdsEnum() {
                private long ord = -1;

                @Override
                public boolean next() {
                    ord++;
                    return ord <= largestKeySeen;
                }

                @Override
                public long value() {
                    return ord + minimumValue;
                }

                @Override
                public long ord() {
                    return ord;
                }
            };
        }

        @Override
        public void close() {
            Releasables.close(alreadySeen);
        }
    }
}
