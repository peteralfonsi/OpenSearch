/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.Randomness;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

public class MultiDimensionCacheStatsTests extends OpenSearchTestCase {
    private final String storeName = "dummy_store";
    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        BytesStreamOutput os = new BytesStreamOutput();
        stats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        MultiDimensionCacheStats deserialized = new MultiDimensionCacheStats(is);

        assertEquals(stats.dimensionNames, deserialized.dimensionNames);
        assertEquals(stats.storeName, deserialized.storeName);

        os = new BytesStreamOutput();
        stats.writeToWithClassName(os);
        is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        CacheStats deserializedViaCacheStats = CacheStats.readFromStreamWithClassName(is);
        assertEquals(MultiDimensionCacheStats.class, deserializedViaCacheStats.getClass());

        assertEquals(stats, deserialized);
        assertEquals(stats, deserializedViaCacheStats);
    }

    public void testEquals() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        StatsHolder differentStoreNameStatsHolder = new StatsHolder(dimensionNames, "nonMatchingStoreName");
        StatsHolder nonMatchingStatsHolder = new StatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(List.of(statsHolder, differentStoreNameStatsHolder), usedDimensionValues, 100, 10);
        populateStats(nonMatchingStatsHolder, usedDimensionValues, 100, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        MultiDimensionCacheStats secondStats = (MultiDimensionCacheStats) statsHolder.getCacheStats();
        assertEquals(stats, secondStats);
        MultiDimensionCacheStats nonMatchingStats = (MultiDimensionCacheStats) nonMatchingStatsHolder.getCacheStats();
        assertNotEquals(stats, nonMatchingStats);
        MultiDimensionCacheStats differentStoreNameStats = (MultiDimensionCacheStats) differentStoreNameStatsHolder.getCacheStats();
        assertNotEquals(stats, differentStoreNameStats);
    }

    public void testAddAndGet() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        // test the value in the map is as expected for each distinct combination of values
        for (List<String> dimensionValues : expected.keySet()) {
            CacheStatsCounter expectedCounter = expected.get(dimensionValues);

            CacheStatsCounterSnapshot actualStatsHolder = StatsHolderTests.getNode(dimensionValues, statsHolder.getStatsRoot())
                .getStatsSnapshot();
            CacheStatsCounterSnapshot actualCacheStats = getNode(dimensionValues, stats.getStatsRoot()).getStatsSnapshot();

            assertEquals(expectedCounter.snapshot(), actualStatsHolder);
            assertEquals(expectedCounter.snapshot(), actualCacheStats);
        }

        // test gets for total (this also checks sum-of-children logic)
        CacheStatsCounter expectedTotal = new CacheStatsCounter();
        for (List<String> dims : expected.keySet()) {
            expectedTotal.add(expected.get(dims));
        }
        assertEquals(expectedTotal.snapshot(), stats.getTotalStats());

        assertEquals(expectedTotal.getHits(), stats.getTotalHits());
        assertEquals(expectedTotal.getMisses(), stats.getTotalMisses());
        assertEquals(expectedTotal.getEvictions(), stats.getTotalEvictions());
        assertEquals(expectedTotal.getSizeInBytes(), stats.getTotalSizeInBytes());
        assertEquals(expectedTotal.getEntries(), stats.getTotalEntries());

        assertSumOfChildrenStats(stats.getStatsRoot());
    }

    public void testEmptyDimsList() throws Exception {
        // If the dimension list is empty, the tree should have only the root node containing the total stats.
        StatsHolder statsHolder = new StatsHolder(List.of(), storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 100);
        populateStats(statsHolder, usedDimensionValues, 10, 100);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        MultiDimensionCacheStats.MDCSDimensionNode statsRoot = stats.getStatsRoot();
        assertEquals(0, statsRoot.children.size());
        assertEquals(stats.getTotalStats(), statsRoot.getStatsSnapshot());
    }

    public void testAggregateByAllDimensions() throws Exception {
        // Aggregating with all dimensions as levels should just give us the same values that were in the original map
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        MultiDimensionCacheStats.MDCSDimensionNode aggregated = stats.aggregateByLevels(dimensionNames);
        for (Map.Entry<List<String>, CacheStatsCounter> expectedEntry : expected.entrySet()) {
            List<String> dimensionValues = new ArrayList<>();
            for (String dimValue : expectedEntry.getKey()) {
                dimensionValues.add(dimValue);
            }
            assertEquals(expectedEntry.getValue().snapshot(), getNode(dimensionValues, aggregated).getStatsSnapshot());
        }
        assertSumOfChildrenStats(aggregated);
    }

    public void testAggregateBySomeDimensions() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3", "dim4");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<List<String>, CacheStatsCounter> expected = populateStats(statsHolder, usedDimensionValues, 1000, 10);
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
                MultiDimensionCacheStats.MDCSDimensionNode aggregated = stats.aggregateByLevels(levels);
                Map<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> aggregatedLeafNodes = getAllLeafNodes(aggregated);

                for (Map.Entry<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> aggEntry : aggregatedLeafNodes.entrySet()) {
                    CacheStatsCounter expectedCounter = new CacheStatsCounter();
                    for (List<String> expectedDims : expected.keySet()) {
                        if (expectedDims.containsAll(aggEntry.getKey())) {
                            expectedCounter.add(expected.get(expectedDims));
                        }
                    }
                    assertEquals(expectedCounter.snapshot(), aggEntry.getValue().getStatsSnapshot());
                }
                assertSumOfChildrenStats(aggregated);
            }
        }
    }

    public void testXContentForLevels() throws Exception {
        List<String> dimensionNames = List.of("A", "B", "C");

        StatsHolder statsHolder = new StatsHolder(dimensionNames, storeName);
        StatsHolderTests.populateStatsHolderFromStatsValueMap(statsHolder, Map.of(
            List.of("A1", "B1", "C1"), new CacheStatsCounter(1, 1, 1, 1, 1),
            List.of("A1", "B1", "C2"), new CacheStatsCounter(2, 2, 2, 2, 2),
            List.of("A1", "B2", "C1"), new CacheStatsCounter(3, 3, 3, 3, 3),
            List.of("A2", "B1", "C3"), new CacheStatsCounter(4, 4, 4, 4, 4)
        ));
        MultiDimensionCacheStats stats = (MultiDimensionCacheStats) statsHolder.getCacheStats();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;

        builder.startObject();
        stats.toXContentForLevels(builder, params, List.of("A", "B", "C"));
        builder.endObject();
        String resultString = builder.toString();
        Map<String, Object> result = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);

        Map<String, BiConsumer<CacheStatsCounter, Integer>> fieldNamesMap = Map.of(
            CacheStatsCounterSnapshot.Fields.MEMORY_SIZE_IN_BYTES,
            (counter, value) -> counter.sizeInBytes.inc(value),
            CacheStatsCounterSnapshot.Fields.EVICTIONS,
            (counter, value) -> counter.evictions.inc(value),
            CacheStatsCounterSnapshot.Fields.HIT_COUNT,
            (counter, value) -> counter.hits.inc(value),
            CacheStatsCounterSnapshot.Fields.MISS_COUNT,
            (counter, value) -> counter.misses.inc(value),
            CacheStatsCounterSnapshot.Fields.ENTRIES,
            (counter, value) -> counter.entries.inc(value)
        );

        Map<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> leafNodes = getAllLeafNodes(stats.getStatsRoot());
        for (Map.Entry<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> entry : leafNodes.entrySet()) {
            List<String> xContentKeys = new ArrayList<>();
            for (int i = 0; i < dimensionNames.size(); i++) {
                xContentKeys.add(dimensionNames.get(i));
                xContentKeys.add(entry.getKey().get(i));
            }
            CacheStatsCounter counterFromXContent = new CacheStatsCounter();

            for (Map.Entry<String, BiConsumer<CacheStatsCounter, Integer>> fieldNamesEntry : fieldNamesMap.entrySet()) {
                List<String> fullXContentKeys = new ArrayList<>(xContentKeys);
                fullXContentKeys.add(fieldNamesEntry.getKey());
                int valueInXContent = (int) getValueFromNestedXContentMap(result, fullXContentKeys);
                BiConsumer<CacheStatsCounter, Integer> incrementer = fieldNamesEntry.getValue();
                incrementer.accept(counterFromXContent, valueInXContent);
            }

            CacheStatsCounterSnapshot expected = entry.getValue().getStatsSnapshot();
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
    private Map<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> getAllLeafNodes(MultiDimensionCacheStats.MDCSDimensionNode root) {
        Map<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> result = new HashMap<>();
        getAllLeafNodesHelper(result, root, new ArrayList<>());
        return result;
    }

    private void getAllLeafNodesHelper(
        Map<List<String>, MultiDimensionCacheStats.MDCSDimensionNode> result,
        MultiDimensionCacheStats.MDCSDimensionNode current,
        List<String> pathToCurrent
    ) {
        if (!current.hasChildren()) {
            result.put(pathToCurrent, current);
        } else {
            for (Map.Entry<String, MultiDimensionCacheStats.MDCSDimensionNode> entry : current.children.entrySet()) {
                List<String> newPath = new ArrayList<>(pathToCurrent);
                newPath.add(entry.getKey());
                getAllLeafNodesHelper(result, entry.getValue(), newPath);
            }
        }
    }

    private void assertSumOfChildrenStats(MultiDimensionCacheStats.MDCSDimensionNode current) {
        if (current.hasChildren()) {
            CacheStatsCounter expectedTotal = new CacheStatsCounter();
            for (MultiDimensionCacheStats.MDCSDimensionNode child : current.children.values()) {
                expectedTotal.add(child.getStatsSnapshot());
            }
            assertEquals(expectedTotal.snapshot(), current.getStatsSnapshot());
            for (MultiDimensionCacheStats.MDCSDimensionNode child : current.children.values()) {
                assertSumOfChildrenStats(child);
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

    static Map<List<String>, CacheStatsCounter> populateStats(
        StatsHolder statsHolder,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) throws InterruptedException {
        return populateStats(List.of(statsHolder), usedDimensionValues, numDistinctValuePairs, numRepetitionsPerValue);
    }

    static Map<List<String>, CacheStatsCounter> populateStats(
        List<StatsHolder> statsHolders,
        Map<String, List<String>> usedDimensionValues,
        int numDistinctValuePairs,
        int numRepetitionsPerValue
    ) throws InterruptedException {
        Map<List<String>, CacheStatsCounter> expected = new ConcurrentHashMap<>();

        for (StatsHolder statsHolder : statsHolders) {
            assertEquals(statsHolders.get(0).getDimensionNames(), statsHolder.getDimensionNames());
        }
        Thread[] threads = new Thread[numDistinctValuePairs];
        CountDownLatch countDownLatch = new CountDownLatch(numDistinctValuePairs);
        Random rand = Randomness.get();
        List<List<String>> dimensionsForThreads = new ArrayList<>();
        for (int i = 0; i < numDistinctValuePairs; i++) {
            dimensionsForThreads.add(getRandomDimList(statsHolders.get(0).getDimensionNames(), usedDimensionValues, true, rand));
            int finalI = i;
            threads[i] = new Thread(() -> {
                Random threadRand = Randomness.get();
                List<String> dimensions = dimensionsForThreads.get(finalI);
                expected.computeIfAbsent(dimensions, (key) -> new CacheStatsCounter());
                for (StatsHolder statsHolder : statsHolders) {
                    for (int j = 0; j < numRepetitionsPerValue; j++) {
                        CacheStatsCounter statsToInc = new CacheStatsCounter(
                            threadRand.nextInt(10),
                            threadRand.nextInt(10),
                            threadRand.nextInt(10),
                            threadRand.nextInt(5000),
                            threadRand.nextInt(10)
                        );
                        expected.get(dimensions).hits.inc(statsToInc.getHits());
                        expected.get(dimensions).misses.inc(statsToInc.getMisses());
                        expected.get(dimensions).evictions.inc(statsToInc.getEvictions());
                        expected.get(dimensions).sizeInBytes.inc(statsToInc.getSizeInBytes());
                        expected.get(dimensions).entries.inc(statsToInc.getEntries());
                        StatsHolderTests.populateStatsHolderFromStatsValueMap(statsHolder, Map.of(dimensions, statsToInc));
                    }
                }
                countDownLatch.countDown();
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        countDownLatch.await();
        return expected;
    }

    private static List<String> getRandomDimList(
        List<String> dimensionNames,
        Map<String, List<String>> usedDimensionValues,
        boolean pickValueForAllDims,
        Random rand
    ) {
        List<String> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            if (pickValueForAllDims || rand.nextBoolean()) { // if pickValueForAllDims, always pick a value for each dimension, otherwise do
                // so 50% of the time
                int index = between(0, usedDimensionValues.get(dimName).size() - 1);
                result.add(usedDimensionValues.get(dimName).get(index));
            }
        }
        return result;
    }

    private MultiDimensionCacheStats.MDCSDimensionNode getNode(
        List<String> dimensionValues,
        MultiDimensionCacheStats.MDCSDimensionNode root
    ) {
        MultiDimensionCacheStats.MDCSDimensionNode current = root;
        for (String dimensionValue : dimensionValues) {
            current = current.getChildren().get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
}
