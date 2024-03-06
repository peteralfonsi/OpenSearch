/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class AggregatedStatsTests extends OpenSearchTestCase {

    private AggregatedStats getPopulatedAggregatedStats(List<String> dimensionNames) {
        AggregatedStats stats = new AggregatedStats(dimensionNames);
        stats.put(List.of("outer_1", "middle_1", "inner_1"), new CacheStatsResponse(1, 1, 1, 1, 1));
        stats.put(List.of("outer_1", "middle_2", "inner_A"), new CacheStatsResponse(2, 2, 2, 2, 2));
        stats.put(List.of("outer_1", "middle_1", "inner_2"), new CacheStatsResponse(3, 3, 3, 3, 3));
        stats.put(List.of("outer_2", "middle_A", "inner_AA"), new CacheStatsResponse(4, 4, 4, 4, 4));
        stats.put(List.of("outer_2", "middle_C", "inner_CC"), new CacheStatsResponse(5, 5, 5, 5, 5));
        stats.put(List.of("outer_2", "middle_B", "inner_BB"), new CacheStatsResponse(6, 6, 6, 6, 6));
        return stats;
    }
    public void testAggregatedStatsWithMaps() throws Exception {
        // Test stats with values in its maps
        List<String> dimensionNames = List.of("outer", "middle", "inner");
        AggregatedStats stats = getPopulatedAggregatedStats(dimensionNames);

        assertEquals(new CacheStatsResponse(1, 1, 1, 1, 1), stats.getResponse(List.of("outer_1", "middle_1", "inner_1")));
        assertEquals(new CacheStatsResponse(2, 2, 2, 2, 2), stats.getResponse(List.of("outer_1", "middle_2", "inner_A")));
        assertEquals(new CacheStatsResponse(3, 3, 3, 3, 3), stats.getResponse(List.of("outer_1", "middle_1", "inner_2")));
        assertEquals(new CacheStatsResponse(4, 4, 4, 4, 4), stats.getResponse(List.of("outer_2", "middle_A", "inner_AA")));
        assertThrows(AssertionError.class, () -> stats.getResponse(List.of("outer_3", "", ""))); // Fails bc "outer_3" has no map associated with it
        assertThrows(AssertionError.class, () -> stats.put(List.of("outer_3", ""), new CacheStatsResponse(0, 0, 0, 0, 0))); // Fails bc the list of dimension values is the wrong length
        assertThrows(AssertionError.class, () -> stats.getResponse(List.of("outer_1", "middle_1", "inner_3"))); // Fails bc "inner_3" has no key associated with it
        assertThrows(AssertionError.class, () -> stats.put(List.of("outer_1", "middle_1", "inner_1"), new CacheStatsResponse(1, 2, 3, 4, 5))); // Fails bc an entry is already present

        assertEquals(List.of("middle_1", "middle_2"), stats.getInnerMapKeySet(List.of("outer_1")));
        assertEquals(List.of("inner_1", "inner_2"), stats.getInnerMapKeySet(List.of("outer_1", "middle_1")));
        assertEquals(List.of("outer_1", "outer_2"), stats.getInnerMapKeySet(List.of()));
        assertEquals(List.of("middle_A", "middle_C", "middle_B"), stats.getInnerMapKeySet(List.of("outer_2")));
        assertEquals(List.of("outer_1", "outer_2"), stats.getInnerMapKeySet(List.of()));
        assertThrows(AssertionError.class, () -> stats.getInnerMapKeySet(List.of("outer_3"))); // Fails bc there is no "outer_3"
        assertThrows(AssertionError.class, () -> stats.getInnerMapKeySet(List.of("outer_1", "middle_1", "inner_1"))); // Fails bc list is too long
        assertEquals(dimensionNames, stats.getDimensionNames());
    }

    public void testAggregatedStatsSerialization() throws Exception {
        List<String> dimensionNames = List.of("outer", "middle", "inner");
        AggregatedStats stats = getPopulatedAggregatedStats(dimensionNames);

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        AggregatedStats deserialized = new AggregatedStats(is);

        assertEquals(stats.getDimensionNames(), deserialized.getDimensionNames());
        assertEquals(new CacheStatsResponse(1, 1, 1, 1, 1), deserialized.getResponse(List.of("outer_1", "middle_1", "inner_1")));
        assertEquals(new CacheStatsResponse(2, 2, 2, 2, 2), deserialized.getResponse(List.of("outer_1", "middle_2", "inner_A")));
        assertEquals(new CacheStatsResponse(3, 3, 3, 3, 3), deserialized.getResponse(List.of("outer_1", "middle_1", "inner_2")));
        assertEquals(new CacheStatsResponse(4, 4, 4, 4, 4), deserialized.getResponse(List.of("outer_2", "middle_A", "inner_AA")));
        assertEquals(deserialized.getInnerMapKeySet(List.of("outer_1")), stats.getInnerMapKeySet(List.of("outer_1")));
        assertEquals(deserialized.getInnerMapKeySet(List.of("outer_1", "middle_1")), stats.getInnerMapKeySet(List.of("outer_1", "middle_1")));
        assertEquals(deserialized.getInnerMapKeySet(List.of()), stats.getInnerMapKeySet(List.of()));
        assertEquals(deserialized.getInnerMapKeySet(List.of("outer_2")), stats.getInnerMapKeySet(List.of("outer_2")));
        assertEquals(deserialized.getInnerMapKeySet(List.of()), stats.getInnerMapKeySet(List.of()));

        assertEquals(stats, deserialized);

        // Test serialization when there are no entries
        AggregatedStats emptyStats = new AggregatedStats(dimensionNames);
        os = new BytesStreamOutput();
        emptyStats.writeTo(os);
        is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        deserialized = new AggregatedStats(is);
        assertEquals(deserialized.getDimensionNames(), emptyStats.getDimensionNames());
        assertEquals(emptyStats, deserialized);
    }
    public void testAggregatedStatsWithOneValue() throws Exception {
        // Test stats with no dimension names (only one value, no inner maps)
        AggregatedStats totalStats = new AggregatedStats(List.of());
        assertEquals(0, totalStats.getSize());
        totalStats.put(List.of(), new CacheStatsResponse(0,0,0,0,0));
        assertEquals(new CacheStatsResponse(0,0,0,0,0), totalStats.getResponse(List.of()));
        assertThrows(AssertionError.class, () -> totalStats.getInnerMapKeySet(List.of()));
        assertEquals(1, totalStats.getSize());

        // Test serialization in this case
        BytesStreamOutput os = new BytesStreamOutput();
        totalStats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        AggregatedStats deserializedTotalStats = new AggregatedStats(is);

        assertEquals(totalStats.getDimensionNames(), deserializedTotalStats.getDimensionNames());
        assertEquals(new CacheStatsResponse(0,0,0,0,0), deserializedTotalStats.getResponse(List.of()));
        assertThrows(AssertionError.class, () -> deserializedTotalStats.getInnerMapKeySet(List.of()));
        assertEquals(totalStats, deserializedTotalStats);

        // Test empty stats serialization
        AggregatedStats emptyTotalStats = new AggregatedStats(List.of());
        os = new BytesStreamOutput();
        emptyTotalStats.writeTo(os);
        is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        AggregatedStats deserialized = new AggregatedStats(is);
        assertEquals(deserialized.getDimensionNames(), emptyTotalStats.getDimensionNames());
        assertEquals(emptyTotalStats, deserialized);
    }

    public void testAggregatedStatsAdd() throws Exception {
        // Test for nested maps case
        List<String> dimensionNames = List.of("outer", "middle", "inner");
        AggregatedStats stats = new AggregatedStats(dimensionNames);

        stats.put(List.of("outer_1", "middle_1", "inner_1"), new CacheStatsResponse(1, 1, 1, 1, 1));
        stats.addTo(List.of("outer_1", "middle_1", "inner_1"), new CacheStatsResponse(1,2,3,4,5));
        assertEquals(new CacheStatsResponse(2,3,4,5,6), stats.getResponse(List.of("outer_1", "middle_1", "inner_1")));

        assertThrows(AssertionError.class, () -> stats.addTo(List.of("outer_1", "middle_1", "inner_2"), new CacheStatsResponse(1,1,1,1,1))); // Fails bc no value for these keys

        // Test for total stats case
        AggregatedStats totalStats = new AggregatedStats(List.of());
        assertThrows(AssertionError.class, () -> stats.addTo(List.of(), new CacheStatsResponse(1,2,3,4,5))); // Fails bc no value
        totalStats.put(List.of(), new CacheStatsResponse(1,2,3,4,5));
        totalStats.addTo(List.of(), new CacheStatsResponse(1,1,1,1,1));
        assertEquals(new CacheStatsResponse(2,3,4,5,6), totalStats.getResponse(List.of()));
    }

    public void testGetSummedResponse() throws Exception {
        List<String> dimensionNames = List.of("outer", "middle", "inner");
        AggregatedStats stats = new AggregatedStats(dimensionNames);
        stats.put(List.of("outer_1", "middle_1", "inner_1"), new CacheStatsResponse(1, 1, 1, 1, 1));
        stats.put(List.of("outer_1", "middle_2", "inner_A"), new CacheStatsResponse(2, 2, 2, 2, 2));
        stats.put(List.of("outer_1", "middle_1", "inner_2"), new CacheStatsResponse(3, 3, 3, 3, 3));
        stats.put(List.of("outer_2", "middle_A", "inner_AA"), new CacheStatsResponse(4, 4, 4, 4, 4));
        stats.put(List.of("outer_2", "middle_C", "inner_CC"), new CacheStatsResponse(5, 5, 5, 5, 5));
        stats.put(List.of("outer_2", "middle_B", "inner_BB"), new CacheStatsResponse(6, 6, 6, 6, 6));
        stats.put(List.of("outer_1", "middle_2", "inner_1"), new CacheStatsResponse(7,7,7,7,7));
        stats.put(List.of("outer_2", "middle_2", "inner_1"), new CacheStatsResponse(8,8,8,8,8));

        CacheStatsResponse response;
        List<String> addMiddleDimension = new ArrayList<>();
        addMiddleDimension.add("outer_1"); // Add one at a time as List.of() doesn't accept null
        addMiddleDimension.add(null);
        addMiddleDimension.add("inner_1");
        response = stats.getSummedResponse(addMiddleDimension);
        assertEquals(new CacheStatsResponse(8, 8, 8, 8,8), response);

        List<String> matchOnlyOneInner = new ArrayList<>(); // Only one matching inner dim
        matchOnlyOneInner.add(null); // Add one at a time as List.of() doesn't accept null
        matchOnlyOneInner.add(null);
        matchOnlyOneInner.add("inner_2");
        response = stats.getSummedResponse(matchOnlyOneInner);
        assertEquals(new CacheStatsResponse(3,3,3,3,3), response);

        List<String> allInner1 = new ArrayList<>();
        allInner1.add(null);
        allInner1.add(null);
        allInner1.add("inner_1");
        response = stats.getSummedResponse(allInner1);
        assertEquals(new CacheStatsResponse(16,16,16,16,16), response);

        List<String> exact = List.of("outer_1", "middle_1", "inner_1");
        assertEquals(new CacheStatsResponse(1,1,1,1,1), stats.getSummedResponse(exact));

        List<String> anyInner = new ArrayList<>();
        anyInner.add("outer_1");
        anyInner.add("middle_1");
        anyInner.add(null);
        response = stats.getSummedResponse(anyInner);
        assertEquals(new CacheStatsResponse(4,4,4,4,4), response);

        List<String> all = new ArrayList<>();
        all.add(null);
        all.add(null);
        all.add(null);
        response = stats.getSummedResponse(all);
        assertEquals(new CacheStatsResponse(36, 36, 36, 36, 36), response);

        List<String> matchForOnlyOneOuter = new ArrayList<>();
        matchForOnlyOneOuter.add(null);
        matchForOnlyOneOuter.add("middle_A");
        matchForOnlyOneOuter.add(null);
        response = stats.getSummedResponse(matchForOnlyOneOuter);
        assertEquals(new CacheStatsResponse(4,4,4,4,4), response);

        List<String> noMatching = new ArrayList<>();
        noMatching.add("outer_1");
        noMatching.add("middle_A");
        noMatching.add(null);
        response = stats.getSummedResponse(noMatching);
        assertEquals(new CacheStatsResponse(0,0,0,0,0), response);
    }
}
