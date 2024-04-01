/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);

        assertEquals(stats.snapshot, deserialized.snapshot);
        assertEquals(stats.dimensionNames, deserialized.dimensionNames);

        os = new BytesStreamOutput();
        stats.writeToWithClassName(os);
        is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        CacheStats deserializedViaCacheStats = CacheStats.readFromStreamWithClassName(is);
        assertEquals(MultiDimensionCacheStats.class, deserializedViaCacheStats.getClass());
    }

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<CacheStatsDimension>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        // test the value in the map is as expected for each distinct combination of values
        for (List<CacheStatsDimension> dims : expected.keySet()) {
            CacheStatsCounter expectedCounter = expected.get(dims);
            StatsHolder.Key key = new StatsHolder.Key(StatsHolder.getOrderedDimensions(dims, dimensionNames));
            CounterSnapshot actual = stats.snapshot.get(key);

            assertEquals(expectedCounter.snapshot(), actual);
        }

        // test gets for total
        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (List<CacheStatsDimension> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.snapshot(), stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the map should have only one entry, from the empty set -> the total stats.
        StatsHolder statsHolder = new StatsHolder(List.of());
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 100);
        populateStats(statsHolder, usedDimensionValues, 10, 100);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        assertEquals(1, stats.snapshot.size());
        assertEquals(stats.getTotalStats(), stats.snapshot.get(new StatsHolder.Key(List.of())));
    }

    public void testAggregateByAllDimensions() throws Exception {
        // Aggregating with all dimensions as levels should just give us the same values that were in the original map
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<CacheStatsDimension>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        MultiDimensionCacheStats.DimensionNode aggregated = stats.aggregateByLevels(dimensionNames);
        for (Map.Entry<List<CacheStatsDimension>, CacheStatsCounter> expectedEntry : expected.entrySet()) {
            List<String> dimensionValues = new ArrayList<>();
            for (CacheStatsDimension dim : expectedEntry.getKey()) {
                dimensionValues.add(dim.dimensionValue);
            }
            assertEquals(expectedEntry.getValue().snapshot(), aggregated.getNode(dimensionValues).getSnapshot());
        }
    }

    public void testAggregateBySomeDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<CacheStatsDimension>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        for (int i = 0; i < (1 << dimensionNames.size()); i++) {
            // Test each combination of possible levels
            List<String> levels = new ArrayList<>();
            for (int nameIndex = 0; nameIndex < dimensionNames.size(); nameIndex++) {
                if ((i & (1 << nameIndex)) != 0) {
                    levels.add(dimensionNames.get(nameIndex));
                }
            }
            if (levels.size() == 0) {
                assertThrows(IllegalArgumentException.class, () -> stats.aggregateByLevels(levels));
            } else {
                MultiDimensionCacheStats.DimensionNode aggregated = stats.aggregateByLevels(levels);
                Map<List<String>, MultiDimensionCacheStats.DimensionNode> aggregatedLeafNodes = getAllLeafNodes(aggregated);

                for (Map.Entry<List<String>, MultiDimensionCacheStats.DimensionNode> aggEntry : aggregatedLeafNodes.entrySet()) {
                    CacheStatsCounter expectedCounter = new CacheStatsCounter();
                    for (List<CacheStatsDimension> expectedDims : expected.keySet()) {
                        List<CacheStatsDimension> orderedDims = StatsHolder.getOrderedDimensions(
                            new ArrayList<>(expectedDims),
                            dimensionNames
                        );
                        List<String> orderedDimValues = new ArrayList<>();
                        for (CacheStatsDimension dim : orderedDims) {
                            orderedDimValues.add(dim.dimensionValue);
                        }
                        if (orderedDimValues.containsAll(aggEntry.getKey())) {
                            expectedCounter.add(expected.get(expectedDims));
                        }
                    }
                    assertEquals(expectedCounter.snapshot(), aggEntry.getValue().getSnapshot());
                }
            }
        }
    }

    public void testXContentForLevels() throws Exception {
        List<String> dimensionNames = List.of("A", "B", "C");
        Map<StatsHolder.Key, CounterSnapshot> snapshot = Map.of(
            new StatsHolder.Key(List.of(
                new CacheStatsDimension("A", "A1"),
                new CacheStatsDimension("B", "B1"),
                new CacheStatsDimension("C", "C1")
            )),
            new CounterSnapshot(1, 1, 1, 1, 1),
            new StatsHolder.Key(List.of(
                new CacheStatsDimension("A", "A1"),
                new CacheStatsDimension("B", "B1"),
                new CacheStatsDimension("C", "C2")
            )),
            new CounterSnapshot(2, 2, 2, 2, 2),
            new StatsHolder.Key(List.of(
                new CacheStatsDimension("A", "A1"),
                new CacheStatsDimension("B", "B2"),
                new CacheStatsDimension("C", "C1")
            )),
            new CounterSnapshot(3, 3, 3, 3, 3),
            new StatsHolder.Key(List.of(
                new CacheStatsDimension("A", "A2"),
                new CacheStatsDimension("B", "B1"),
                new CacheStatsDimension("C", "C3")
            )),
            new CounterSnapshot(4, 4, 4, 4, 4)
        );
        MultiDimensionCacheStats stats = new MultiDimensionCacheStats(snapshot, dimensionNames);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;

        builder.startObject();
        stats.toXContentForLevels(builder, params, List.of("A", "B", "C"));
        builder.endObject();
        String resultString = builder.toString();
        Map<String, Object> result = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);

        Map<String, BiConsumer<CacheStatsCounter, Integer>> fieldNamesMap = Map.of(
            CounterSnapshot.Fields.MEMORY_SIZE_IN_BYTES,
            (counter, value) -> counter.sizeInBytes.inc(value),
            CounterSnapshot.Fields.EVICTIONS,
            (counter, value) -> counter.evictions.inc(value),
            CounterSnapshot.Fields.HIT_COUNT,
            (counter, value) -> counter.hits.inc(value),
            CounterSnapshot.Fields.MISS_COUNT,
            (counter, value) -> counter.misses.inc(value),
            CounterSnapshot.Fields.ENTRIES,
            (counter, value) -> counter.entries.inc(value)
        );

        for (Map.Entry<StatsHolder.Key, CounterSnapshot> entry : snapshot.entrySet()) {
            List<String> xContentKeys = new ArrayList<>();
            for (int i = 0; i < dimensionNames.size(); i++) {
                xContentKeys.add(dimensionNames.get(i));
                xContentKeys.add(entry.getKey().dimensions.get(i).dimensionValue);
            }
            CacheStatsCounter counterFromXContent = new CacheStatsCounter();

            for (Map.Entry<String, BiConsumer<CacheStatsCounter, Integer>> fieldNamesEntry : fieldNamesMap.entrySet()) {
                List<String> fullXContentKeys = new ArrayList<>(xContentKeys);
                fullXContentKeys.add(fieldNamesEntry.getKey());
                int valueInXContent = (int) getValueFromNestedXContentMap(result, fullXContentKeys);
                BiConsumer<CacheStatsCounter, Integer> incrementer = fieldNamesEntry.getValue();
                incrementer.accept(counterFromXContent, valueInXContent);
            }

            CounterSnapshot expected = entry.getValue();
            assertEquals(counterFromXContent.snapshot(), expected);
        }
    }

    public static Object getValueFromNestedXContentMap(Map<String, Object> xContentMap, List<String> keys) {
        Map<String, Object> current = xContentMap;
        for (int i = 0; i < keys.size() - 1; i++) {
            Object next = current.get(keys.get(i));
            if (next == null) {
                return null;
            }
            current = (Map<String, Object>) next;
        }
        return current.get(keys.get(keys.size() - 1));
    }

    // Get a map from the list of dimension values to the corresponding leaf node.
    private Map<List<String>, MultiDimensionCacheStats.DimensionNode> getAllLeafNodes(MultiDimensionCacheStats.DimensionNode root) {
        Map<List<String>, MultiDimensionCacheStats.DimensionNode> result = new HashMap<>();
        getAllLeafNodesHelper(result, root, new ArrayList<>());
        return result;
    }

    private void getAllLeafNodesHelper(
        Map<List<String>, MultiDimensionCacheStats.DimensionNode> result,
        MultiDimensionCacheStats.DimensionNode current,
        List<String> pathToCurrent
    ) {
        if (current.children.isEmpty()) {
            result.put(pathToCurrent, current);
        } else {
            for (Map.Entry<String, MultiDimensionCacheStats.DimensionNode> entry : current.children.entrySet()) {
                List<String> newPath = new ArrayList<>(pathToCurrent);
                newPath.add(entry.getKey());
                getAllLeafNodesHelper(result, entry.getValue(), newPath);
            }
        }
    }

    static Map<String, List<String>> getUsedDimensionValues(StatsHolder statsHolder, int numValuesPerDim) {
        Map<String, List<String>> usedDimensionValues = new HashMap<>();
        for (int i = 0; i < statsHolder.getDimensionNames().size(); i++) {
            List<String> values = new ArrayList<>();
            for (int j = 0; j < numValuesPerDim; j++) {
                values.add(UUID.randomUUID().toString());
            }
            usedDimensionValues.put(statsHolder.getDimensionNames().get(i), values);
        }
        return usedDimensionValues;
    }

    static Map<List<CacheStatsDimension>, CacheStatsCounter> populateStats(
        StatsHolder statsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) {
        Map<List<CacheStatsDimension>, CacheStatsCounter> expected = new HashMap<>();

        Random rand = Randomness.get();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            List<CacheStatsDimension> dimensions = getRandomDimList(statsHolder.getDimensionNames(), usedDimensionValues, true, rand);
            if (expected.get(dimensions) == null) {
                expected.put(dimensions, new CacheStatsCounter());
            }
            ICacheKey<String> dummyKey = getDummyKey(dimensions);

            for (int j = 0; j < numRepetitionsPerValue; j++) {

                int numHitIncrements = rand.nextInt(10);
                for (int k = 0; k < numHitIncrements; k++) {
                    statsHolder.incrementHits(dummyKey);
                    expected.get(dimensions).hits.inc();
                }

                int numMissIncrements = rand.nextInt(10);
                for (int k = 0; k < numMissIncrements; k++) {
                    statsHolder.incrementMisses(dummyKey);
                    expected.get(dimensions).misses.inc();
                }

                int numEvictionIncrements = rand.nextInt(10);
                for (int k = 0; k < numEvictionIncrements; k++) {
                    statsHolder.incrementEvictions(dummyKey);
                    expected.get(dimensions).evictions.inc();
                }

                int numMemorySizeIncrements = rand.nextInt(10);
                for (int k = 0; k < numMemorySizeIncrements; k++) {
                    long memIncrementAmount = rand.nextInt(5000);
                    statsHolder.incrementSizeInBytes(dummyKey, memIncrementAmount);
                    expected.get(dimensions).sizeInBytes.inc(memIncrementAmount);
                }

                int numEntryIncrements = rand.nextInt(9) + 1;
                for (int k = 0; k < numEntryIncrements; k++) {
                    statsHolder.incrementEntries(dummyKey);
                    expected.get(dimensions).entries.inc();
                }

                int numEntryDecrements = rand.nextInt(numEntryIncrements);
                for (int k = 0; k < numEntryDecrements; k++) {
                    statsHolder.decrementEntries(dummyKey);
                    expected.get(dimensions).entries.dec();
                }
            }
        }
        return expected;
    }

    private static ICacheKey<String> getDummyKey(List<CacheStatsDimension> dims) {
        return new ICacheKey<>(null, dims);
    }

    private static List<CacheStatsDimension> getRandomDimList(
        List<String> dimensionNames,
        Map<String, List<String>> usedDimensionValues,
        boolean pickValueForAllDims,
        Random rand
    ) {
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            if (pickValueForAllDims || rand.nextBoolean()) { // if pickValueForAllDims, always pick a value for each dimension, otherwise do
                // so 50% of the time
                int index = between(0, usedDimensionValues.get(dimName).size() - 1);
                result.add(new CacheStatsDimension(dimName, usedDimensionValues.get(dimName).get(index)));
            }
        }
        return result;
    }
}
