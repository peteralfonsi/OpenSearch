/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.stats.ImmutableCacheStats;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.function.ToLongBiFunction;

public class CaffeineHeapCacheTests extends OpenSearchTestCase {

    private final String dimensionName = "shardId";
    private static final int CACHE_SIZE_IN_BYTES = 1024 * 101;
    private static final int MOCK_WEIGHT = 10;

    public void testBasicGetAndPut() throws IOException {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run) // Specify direct (same thread) executor for testing purposes.
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(10, 100);
        Map<String, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            ICacheKey<String> iCacheKey = getICacheKey(entry.getKey());
            caffeineTest.put(iCacheKey, entry.getValue());
        }
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            String value = caffeineTest.get(getICacheKey(entry.getKey()));
            assertEquals(entry.getValue(), value);
        }
        assertEquals(randomKeys, caffeineTest.stats().getTotalItems());
        assertEquals(randomKeys, caffeineTest.stats().getTotalHits());
        assertEquals(randomKeys, caffeineTest.count());

        // Validate misses
        int expectedNumberOfMisses = randomIntBetween(10, 200);
        for (int i = 0; i < expectedNumberOfMisses; i++) {
            caffeineTest.get(getICacheKey(UUID.randomUUID().toString()));
        }
        assertEquals(expectedNumberOfMisses, caffeineTest.stats().getTotalMisses());
    }

    public void testConcurrentGet() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(20, 100);
        Thread[] threads = new Thread[randomKeys];
        Phaser phaser = new Phaser(randomKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int j = 0;
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
        }
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            caffeineTest.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                assertEquals(entry.getValue(), caffeineTest.get(entry.getKey()));
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel gets above.
        countDownLatch.await(); // Wait for all threads to finish
        assertEquals(randomKeys, caffeineTest.stats().getTotalHits());
    }

    public void testConcurrentPut() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(20, 100);
        Thread[] threads = new Thread[randomKeys];
        Phaser phaser = new Phaser(randomKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int j = 0;
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
        }
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                caffeineTest.put(entry.getKey(), entry.getValue());
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
        countDownLatch.await(); // Wait for all threads to finish
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            String value = caffeineTest.get(entry.getKey());
            assertEquals(entry.getValue(), value);
        }
        assertEquals(randomKeys, caffeineTest.count());
        assertEquals(randomKeys, caffeineTest.stats().getTotalItems());
    }

    // Modified from EhCacheDiskCacheTests.java
    public void testComputeIfAbsentConcurrently() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int numberOfRequest = randomIntBetween(200, 400);
        String key = UUID.randomUUID().toString();
        String value = "dummy";
        Thread[] threads = new Thread[numberOfRequest];
        Phaser phaser = new Phaser(numberOfRequest + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfRequest);

        List<LoadAwareCacheLoader<ICacheKey<String>, String>> loadAwareCacheLoaderList = new CopyOnWriteArrayList<>();

        // Try to hit different request with the same key concurrently. Verify value is only loaded once.
        for (int i = 0; i < numberOfRequest; i++) {
            threads[i] = new Thread(() -> {
                LoadAwareCacheLoader<ICacheKey<String>, String> loadAwareCacheLoader = new LoadAwareCacheLoader<>() {
                    boolean isLoaded;

                    @Override
                    public boolean isLoaded() {
                        return isLoaded;
                    }

                    @Override
                    public String load(ICacheKey<String> key) {
                        isLoaded = true;
                        return value;
                    }
                };
                loadAwareCacheLoaderList.add(loadAwareCacheLoader);
                phaser.arriveAndAwaitAdvance();
                try {
                    assertEquals(value, caffeineTest.computeIfAbsent(getICacheKey(key), loadAwareCacheLoader));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        int numberOfTimesValueLoaded = 0;
        for (int i = 0; i < numberOfRequest; i++) {
            if (loadAwareCacheLoaderList.get(i).isLoaded()) {
                numberOfTimesValueLoaded++;
            }
        }
        assertEquals(1, numberOfTimesValueLoaded);
        assertEquals(1, caffeineTest.stats().getTotalMisses());
        assertEquals(1, caffeineTest.stats().getTotalItems());
        assertEquals(numberOfRequest - 1, caffeineTest.stats().getTotalHits());
        assertEquals(1, caffeineTest.count());
        caffeineTest.close();
    }

    public void testInvalidateAll() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(10, 100);
        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < randomKeys; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            String value = UUID.randomUUID().toString();
            keyValueMap.put(key, value);
            caffeineTest.put(key, value);
        }
        caffeineTest.invalidateAll(); // Clear all the entries.
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            // Verify that value is null for a removed entry.
            assertNull(caffeineTest.get(entry.getKey()));
        }
        assertEquals(0, caffeineTest.count());
        assertEquals(0, caffeineTest.stats().getTotalSizeInBytes());
    }

    // Modified from OpenSearchOnHeapCache.java
    public void testInvalidateWithDropDimensions() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        List<String> dimensionNames = List.of("dim1", "dim2");
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(dimensionNames)
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        List<ICacheKey<String>> keysAdded = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            ICacheKey<String> key = new ICacheKey<>(UUID.randomUUID().toString(), getRandomDimensions(dimensionNames));
            keysAdded.add(key);
            caffeineTest.put(key, UUID.randomUUID().toString());
        }

        ICacheKey<String> keyToDrop = keysAdded.get(0);

        String[] levels = dimensionNames.toArray(new String[0]);
        ImmutableCacheStats snapshot = caffeineTest.stats(levels).getStatsForDimensionValues(keyToDrop.dimensions);
        assertNotNull(snapshot);

        keyToDrop.setDropStatsForDimensions(true);
        caffeineTest.invalidate(keyToDrop);

        // Now assert the stats are gone for any key that has this combination of dimensions, but still there otherwise
        for (ICacheKey<String> keyAdded : keysAdded) {
            snapshot = caffeineTest.stats(levels).getStatsForDimensionValues(keyAdded.dimensions);
            if (keyAdded.dimensions.equals(keyToDrop.dimensions)) {
                assertNull(snapshot);
            } else {
                assertNotNull(snapshot);
            }
        }
    }

    public void testInvalidateConcurrently() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(10, 100);
        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
        }
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            caffeineTest.put(entry.getKey(), entry.getValue());
        }
        assertEquals(keyValueMap.size(), caffeineTest.count());
        List<ICacheKey<String>> removedKeyList = new ArrayList<>();
        Thread[] threads = new Thread[randomKeys];
        Phaser phaser = new Phaser(randomKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
        int j = 0;
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                if (randomBoolean()) {
                    removedKeyList.add(entry.getKey());
                    caffeineTest.invalidate(entry.getKey());
                }
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel invalidations above.
        countDownLatch.await();

        for (ICacheKey<String> removedKey : removedKeyList) {
            assertNull(caffeineTest.get(removedKey));
        }
        assertEquals(keyValueMap.size() - removedKeyList.size(), caffeineTest.count());
        assertEquals((keyValueMap.size() - removedKeyList.size()) * 10, caffeineTest.stats().getTotalSizeInBytes());
    }

    public void testEvictions() throws Exception {
        int MAX_CACHE_SIZE = 100; // Restrict cache size in order to test size-based eviction.
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(getMockDimensions())
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(MAX_CACHE_SIZE)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(MAX_CACHE_SIZE / MOCK_WEIGHT + 1, 100);
        for (int i = 0; i < randomKeys; i++) {
            caffeineTest.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
        }
        assertEquals(randomKeys - (MAX_CACHE_SIZE / MOCK_WEIGHT), caffeineTest.stats().getTotalEvictions());
        assertEquals(randomKeys - (MAX_CACHE_SIZE / MOCK_WEIGHT), removalListener.evictionMetric.count());
        assertEquals(MAX_CACHE_SIZE, caffeineTest.stats().getTotalSizeInBytes());
    }

    public void testConcurrentEvictions() throws Exception {
        int MAX_CACHE_SIZE = 100; // Restrict cache size in order to test size-based eviction.
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(MAX_CACHE_SIZE)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();
        int randomKeys = randomIntBetween(MAX_CACHE_SIZE / MOCK_WEIGHT + 1, 100);
        Thread[] threads = new Thread[randomKeys];
        Phaser phaser = new Phaser(randomKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int j = 0;
        for (int i = 0; i < randomKeys; i++) {
            keyValueMap.put(getICacheKey(UUID.randomUUID().toString()), UUID.randomUUID().toString());
        }
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                caffeineTest.put(entry.getKey(), entry.getValue());
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
        countDownLatch.await(); // Wait for all threads to finish

        ((CaffeineHeapCache<String, String>) caffeineTest).cleanUp(); // Manually perform maintenance cycle, which includes removing
                                                                      // expired entries.
        assertEquals(randomKeys - (MAX_CACHE_SIZE / MOCK_WEIGHT), caffeineTest.stats().getTotalEvictions());
        assertEquals(randomKeys - (MAX_CACHE_SIZE / MOCK_WEIGHT), removalListener.evictionMetric.count());
        assertEquals(MAX_CACHE_SIZE, caffeineTest.stats().getTotalSizeInBytes());
    }

    public void testReplace() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(true);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();

        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int randomKeys = randomIntBetween(10, 100);
        for (int i = 0; i < randomKeys; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            String value = Integer.toString(randomIntBetween(1, 10));
            keyValueMap.put(key, value);
            caffeineTest.put(key, value);
        }

        // Replace old values with new, differently-sized values. Ensure that size changes accordingly.
        for (Map.Entry<ICacheKey<String>, String> entry : keyValueMap.entrySet()) {
            long current_size = caffeineTest.stats().getTotalItems();
            long current_size_in_bytes = caffeineTest.stats().getTotalSizeInBytes();
            String old_value = entry.getValue();
            ICacheKey<String> key = entry.getKey();
            String new_value = Integer.toString(randomIntBetween(1, 10));
            caffeineTest.put(key, new_value);
            keyValueMap.put(key, new_value);
            assertEquals(current_size, caffeineTest.stats().getTotalItems());
            assertEquals(current_size_in_bytes - Integer.parseInt(old_value) + Integer.parseInt(new_value), caffeineTest.stats().getTotalSizeInBytes());
        }
    }

    public void testIteratorRemove() throws Exception {
        ToLongBiFunction<ICacheKey<String>, String> weigher = getMockWeigher(false);
        MockRemovalListener<String, String> removalListener = new MockRemovalListener<>();
        ICache<String, String> caffeineTest = new CaffeineHeapCache.Builder<String, String>().setDimensionNames(List.of(dimensionName))
            .setExecutor(Runnable::run)
            .setExpireAfterAccess(TimeValue.MAX_VALUE)
            .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
            .setWeigher(weigher)
            .setRemovalListener(removalListener)
            .build();

        Map<ICacheKey<String>, String> keyValueMap = new HashMap<>();
        int randomKeys = randomIntBetween(10, 100);
        for (int i = 0; i < randomKeys; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            String value = UUID.randomUUID().toString();
            keyValueMap.put(key, value);
            caffeineTest.put(key, value);
        }

        // Ensures that calling remove() on the result of caffeineTest.keys().iterator() removes key from underlying cache.
        Iterator<ICacheKey<String>> caffeineTestIterator = caffeineTest.keys().iterator();
        while (caffeineTestIterator.hasNext()) {
            ICacheKey<String> next = caffeineTestIterator.next();
            assertEquals(keyValueMap.get(next), caffeineTest.get(next));
            caffeineTestIterator.remove();
            assertNull(caffeineTest.get(next));
        }
        assertEquals(0, caffeineTest.count());
        assertEquals(0, caffeineTest.stats().getTotalSizeInBytes());
        assertEquals(0, caffeineTest.stats().getTotalEvictions()); // This shouldn't increment number of evictions.
    }

    private List<String> getMockDimensions() {
        return List.of("0");
    }

    private ICacheKey<String> getICacheKey(String key) {
        return new ICacheKey<>(key, getMockDimensions());
    }

    private List<String> getRandomDimensions(List<String> dimensionNames) {
        Random rand = Randomness.get();
        int bound = 3;
        List<String> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            result.add(String.valueOf(rand.nextInt(bound)));
        }
        return result;
    }

    private ToLongBiFunction<ICacheKey<String>, String> getMockWeigher(boolean variedWeights) {
        if (!variedWeights) {
            return (iCacheKey, value) -> { return MOCK_WEIGHT; };
        }

        // Used for testing replace.
        return (iCacheKey, value) -> { return Long.parseLong(value); };
    }

    static class MockRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {
        CounterMetric evictionMetric = new CounterMetric();

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            evictionMetric.inc();
        }
    }
}
