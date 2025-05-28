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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class LongKeyedBucketOrdsTests extends OpenSearchTestCase {
    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testExplicitCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE));
    }

    public void testSurpriseCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY));
    }

    public void testMinimumAwareBucketOrds() {
        // collectsFromSingleBucketCase isn't appropriate here since incoming values are mapped more directly to
        // ordinals, rather than being based on order they are first seen
        long minValue = randomLongBetween(-1000, 1000);
        try (LongKeyedBucketOrds.MinimumAwareBucketOrds ords = new LongKeyedBucketOrds.MinimumAwareBucketOrds(minValue, bigArrays)) {
            Set<Long> seen = new HashSet<>();
            long largestSeen = Long.MIN_VALUE;

            // Add some random values all above minValue
            for (int i = 0; i < 1000; i++) {
                long value = randomLongBetween(minValue, minValue + 2000);
                if (value > largestSeen) {
                    largestSeen = value;
                }
                boolean alreadySeen = seen.contains(value);
                long ord = ords.add(0, value);
                long expectedOrd = alreadySeen ? -1 - (value - minValue) : value - minValue;
                assertEquals(expectedOrd, ord);
                assertEquals(ords.size(), largestSeen - minValue + 1);
                seen.add(value);
            }

            // check values below minValue throw AssertionError
            long illegalValue = minValue - randomLongBetween(1, 100);
            assertThrows(AssertionError.class, () -> ords.add(0, illegalValue));

            // Check counting values
            assertEquals(largestSeen - minValue + 1, ords.bucketsInOrd(0));

            // check iteration
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            assertTrue(ordsEnum.next());
            for (int expectedOrd = 0; expectedOrd < ords.bucketsInOrd(0); expectedOrd++) {
                assertEquals(expectedOrd, ordsEnum.ord());
                assertEquals(expectedOrd + minValue, ordsEnum.value());
                if (expectedOrd < ords.bucketsInOrd(0) - 1) {
                    assertTrue(ordsEnum.next());
                } else {
                    assertFalse(ordsEnum.next());
                }
            }

            assertEquals(0, ords.maxOwningBucketOrd());
        }
    }

    public void testMinimumAwareBucketOrdsResizing() {
        long minValue = 5;
        int size = (int) LongKeyedBucketOrds.MinimumAwareBucketOrds.DEFAULT_INITIAL_CAPACITY + 4;
        try (LongKeyedBucketOrds.MinimumAwareBucketOrds ords = new LongKeyedBucketOrds.MinimumAwareBucketOrds(minValue, bigArrays)) {
            // Check at creation, all entries are 0
            for (int i = 0; i < ords.alreadySeen.size(); i++) {
                assertEquals(0, ords.alreadySeen.get(i));
            }

            // add enough values to trigger 1 resize
            for (int i = 0; i < size; i++) {
                ords.add(0, (long) minValue + i);
            }
            assertEquals(ords.size(), size);
            assertEquals(ords.alreadySeen.size(), Numbers.nextPowerOfTwo(size));

            // Check new entries are initialized to 0
            for (int i = size; i < ords.alreadySeen.size(); i++) {
                assertEquals(0, ords.alreadySeen.get(i));
            }
            // Check original entries are maintained
            for (int i = 0; i < size; i++) {
                assertEquals(1, ords.alreadySeen.get(i));
            }
        }
    }

    private void collectsFromSingleBucketCase(LongKeyedBucketOrds ords) {
        try {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(0, 1000), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(0, 1000), equalTo(1L));

            // And some random values
            Set<Long> seen = new HashSet<>();
            seen.add(0L);
            seen.add(1000L);
            assertThat(ords.size(), equalTo(2L));
            long[] values = new long[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, OpenSearchTestCase::randomLong);
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(0, values[i]), equalTo(-1L));
                assertThat(ords.add(0, values[i]), equalTo(i + 2L));
                assertThat(ords.find(0, values[i]), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, 0), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(0, values[i]), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));

            // Check counting values
            assertThat(ords.bucketsInOrd(0), equalTo(values.length + 2L));

            // Check iteration
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(0L));
            assertThat(ordsEnum.value(), equalTo(0L));
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(1L));
            assertThat(ordsEnum.value(), equalTo(1000L));
            for (int i = 0; i < values.length; i++) {
                assertTrue(ordsEnum.next());
                assertThat(ordsEnum.ord(), equalTo(i + 2L));
                assertThat(ordsEnum.value(), equalTo(values[i]));
            }
            assertFalse(ordsEnum.next());

            assertThat(ords.maxOwningBucketOrd(), equalTo(0L));
        } finally {
            ords.close();
        }
    }

    public void testCollectsFromManyBuckets() {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(1, 0), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));
            assertThat(ords.size(), equalTo(2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(1, 0), equalTo(1L));

            // And some random values
            Set<OwningBucketOrdAndValue> seen = new HashSet<>();
            seen.add(new OwningBucketOrdAndValue(0, 0));
            seen.add(new OwningBucketOrdAndValue(1, 0));
            OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
            long maxAllowedOwningBucketOrd = scaledRandomIntBetween(0, values.length);
            long maxOwningBucketOrd = Long.MIN_VALUE;
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(
                    seen::contains,
                    () -> new OwningBucketOrdAndValue(randomLongBetween(0, maxAllowedOwningBucketOrd), randomLong())
                );
                seen.add(values[i]);
                maxOwningBucketOrd = Math.max(maxOwningBucketOrd, values[i].owningBucketOrd);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(-1L));
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, 0), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));

            for (long owningBucketOrd = 0; owningBucketOrd <= maxAllowedOwningBucketOrd; owningBucketOrd++) {
                long expectedCount = 0;
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(owningBucketOrd);
                if (owningBucketOrd <= 1) {
                    expectedCount++;
                    assertTrue(ordsEnum.next());
                    assertThat(ordsEnum.ord(), equalTo(owningBucketOrd));
                    assertThat(ordsEnum.value(), equalTo(0L));
                }
                for (int i = 0; i < values.length; i++) {
                    if (values[i].owningBucketOrd == owningBucketOrd) {
                        expectedCount++;
                        assertTrue(ordsEnum.next());
                        assertThat(ordsEnum.ord(), equalTo(i + 2L));
                        assertThat(ordsEnum.value(), equalTo(values[i].value));
                    }
                }
                assertFalse(ordsEnum.next());

                assertThat(ords.bucketsInOrd(owningBucketOrd), equalTo(expectedCount));
            }
            assertFalse(ords.ordsEnum(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)).next());
            assertThat(ords.bucketsInOrd(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)), equalTo(0L));

            assertThat(ords.maxOwningBucketOrd(), greaterThanOrEqualTo(maxOwningBucketOrd));
        }
    }

    private class OwningBucketOrdAndValue {
        private final long owningBucketOrd;
        private final long value;

        OwningBucketOrdAndValue(long owningBucketOrd, long value) {
            this.owningBucketOrd = owningBucketOrd;
            this.value = value;
        }

        @Override
        public String toString() {
            return owningBucketOrd + "/" + value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OwningBucketOrdAndValue other = (OwningBucketOrdAndValue) obj;
            return owningBucketOrd == other.owningBucketOrd && value == other.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(owningBucketOrd, value);
        }
    }
}
