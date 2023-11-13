/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class TieredCacheSpilloverStrategyServiceTests extends OpenSearchTestCase {

    public void testComputeAndAbsentWithoutAnyOnHeapCacheEviction() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = intializeTieredCacheService(
            onHeapCacheSize,
            randomIntBetween(1, 4),
            eventListener,
                null,
            null
        );
        int numOfItems1 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        List<String> keys = new ArrayList<>();
        // Put values in cache.
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            keys.add(key);
            TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
            spilloverStrategyService.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, eventListener.enumMap.get(TierType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(TierType.ON_HEAP).hitCount.count());
        assertEquals(0, eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count());

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(1, onHeapCacheSize / 2 - 1);
        int cacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) {
                // Hit cache with stored key
                cacheHit++;
                int index = randomIntBetween(0, keys.size() - 1);
                spilloverStrategyService.computeIfAbsent(keys.get(index), getTieredCacheLoader());
            } else {
                // Hit cache with randomized key which is expected to miss cache always.
                spilloverStrategyService.computeIfAbsent(UUID.randomUUID().toString(), getTieredCacheLoader());
                cacheMiss++;
            }
        }
        assertEquals(cacheHit, eventListener.enumMap.get(TierType.ON_HEAP).hitCount.count());
        assertEquals(numOfItems1 + cacheMiss, eventListener.enumMap.get(TierType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count());
    }

    public void testComputeAndAbsentWithEvictionsFromOnHeapCache() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(60, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;
        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = intializeTieredCacheService(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            null,
            null
        );

        // Put values in cache more than it's size and cause evictions from onHeap.
        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
            spilloverStrategyService.computeIfAbsent(key, tieredCacheLoader);
        }
        assertEquals(numOfItems1, eventListener.enumMap.get(TierType.ON_HEAP).missCount.count());
        assertEquals(0, eventListener.enumMap.get(TierType.ON_HEAP).hitCount.count());
        assertTrue(eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count() > 0);

        assertEquals(
            eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count(),
            eventListener.enumMap.get(TierType.DISK).cachedCount.count()
        );
        assertEquals(diskTierKeys.size(), eventListener.enumMap.get(TierType.DISK).cachedCount.count());

        // Try to hit cache again with some randomization.
        int numOfItems2 = randomIntBetween(50, 200);
        int onHeapCacheHit = 0;
        int diskCacheHit = 0;
        int cacheMiss = 0;
        for (int iter = 0; iter < numOfItems2; iter++) {
            if (randomBoolean()) {
                if (randomBoolean()) { // Hit cache with key stored in onHeap cache.
                    onHeapCacheHit++;
                    int index = randomIntBetween(0, onHeapKeys.size() - 1);
                    spilloverStrategyService.computeIfAbsent(onHeapKeys.get(index), getTieredCacheLoader());
                } else { // Hit cache with key stored in disk cache.
                    diskCacheHit++;
                    int index = randomIntBetween(0, diskTierKeys.size() - 1);
                    spilloverStrategyService.computeIfAbsent(diskTierKeys.get(index), getTieredCacheLoader());
                }
            } else {
                // Hit cache with randomized key which is expected to miss cache always.
                TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
                spilloverStrategyService.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
                cacheMiss++;
            }
        }
        // On heap cache misses would also include diskCacheHits as it means it missed onHeap cache.
        assertEquals(numOfItems1 + cacheMiss + diskCacheHit, eventListener.enumMap.get(TierType.ON_HEAP).missCount.count());
        assertEquals(onHeapCacheHit, eventListener.enumMap.get(TierType.ON_HEAP).hitCount.count());
        assertEquals(cacheMiss + numOfItems1, eventListener.enumMap.get(TierType.DISK).missCount.count());
        assertEquals(diskCacheHit, eventListener.enumMap.get(TierType.DISK).hitCount.count());
    }

    public void testComputeAndAbsentWithEvictionsFromBothTier() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = intializeTieredCacheService(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            null,
            null
        );

        int numOfItems = randomIntBetween(totalSize + 1, totalSize * 3);
        for (int iter = 0; iter < numOfItems; iter++) {
            TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
            spilloverStrategyService.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
        }
        assertTrue(eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count() > 0);
        assertTrue(eventListener.enumMap.get(TierType.DISK).evictionsMetric.count() > 0);
    }

    public void testGetAndCount() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        int diskCacheSize = randomIntBetween(onHeapCacheSize + 1, 100);
        int totalSize = onHeapCacheSize + diskCacheSize;

        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = intializeTieredCacheService(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            null,
            null
        );

        int numOfItems1 = randomIntBetween(onHeapCacheSize + 1, totalSize);
        List<String> onHeapKeys = new ArrayList<>();
        List<String> diskTierKeys = new ArrayList<>();
        for (int iter = 0; iter < numOfItems1; iter++) {
            String key = UUID.randomUUID().toString();
            if (iter > (onHeapCacheSize - 1)) {
                // All these are bound to go to disk based cache.
                diskTierKeys.add(key);
            } else {
                onHeapKeys.add(key);
            }
            TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
            spilloverStrategyService.computeIfAbsent(key, tieredCacheLoader);
        }

        for (int iter = 0; iter < numOfItems1; iter++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    int index = randomIntBetween(0, onHeapKeys.size() - 1);
                    assertNotNull(spilloverStrategyService.get(onHeapKeys.get(index)));
                } else {
                    int index = randomIntBetween(0, diskTierKeys.size() - 1);
                    assertNotNull(spilloverStrategyService.get(diskTierKeys.get(index)));
                }
            } else {
                assertNull(spilloverStrategyService.get(UUID.randomUUID().toString()));
            }
        }
        assertEquals(numOfItems1, spilloverStrategyService.count());
    }

    public void testWithDiskTierNull() throws Exception {
        int onHeapCacheSize = randomIntBetween(10, 30);
        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        Function<String, String> identityFunction = (String value) -> { return value; };
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = new TieredCacheSpilloverStrategyService.Builder<
            String,
            String,
            String>().setOnHeapCachingTier(new MockOnHeapCacheTier<>(onHeapCacheSize))
            .setTieredCacheEventListener(eventListener)
            .withPreDiskCachingPolicyFunction(identityFunction)
            .build();
        int numOfItems = randomIntBetween(onHeapCacheSize + 1, onHeapCacheSize * 3);
        for (int iter = 0; iter < numOfItems; iter++) {
            TieredCacheLoader<String, String> tieredCacheLoader = getTieredCacheLoader();
            spilloverStrategyService.computeIfAbsent(UUID.randomUUID().toString(), tieredCacheLoader);
        }
        assertTrue(eventListener.enumMap.get(TierType.ON_HEAP).evictionsMetric.count() > 0);
        assertEquals(0, eventListener.enumMap.get(TierType.DISK).cachedCount.count());
        assertEquals(0, eventListener.enumMap.get(TierType.DISK).evictionsMetric.count());
        assertEquals(0, eventListener.enumMap.get(TierType.DISK).missCount.count());
    }

    public void testTransformationFunctionAndDiskTierPolicy() throws Exception {
        // For transformation function, return the string reversed
        // For policy function, allow if what it receives starts with "a" (so original string ends with "a") and string is even length
        Function<String, String> reverseStringFunction = (String value) -> {
            return new StringBuilder(value).reverse().toString();
        };
        ArrayList<CacheTierPolicy<String>> policies = new ArrayList<>();
        policies.add(new AllowFirstLetterA());
        policies.add(new AllowEvenLengths());

        int onHeapCacheSize = 0;
        int diskCacheSize = 10000;
        MockTieredCacheEventListener<String, String> eventListener = new MockTieredCacheEventListener<String, String>();
        TieredCacheSpilloverStrategyService<String, String, String> spilloverStrategyService = intializeTieredCacheService(
            onHeapCacheSize,
            diskCacheSize,
            eventListener,
            reverseStringFunction,
            policies
        );

        Map<String, String> keyValuePairs = new HashMap<>();
        Map<String, Boolean> expectedOutputs = new HashMap<>();
        keyValuePairs.put("key1", "dcba");
        expectedOutputs.put("key1", true);
        keyValuePairs.put("key2", "edcba");
        expectedOutputs.put("key2", false);
        keyValuePairs.put("key3", "abc");
        expectedOutputs.put("key3", false);
        keyValuePairs.put("key4", "ab");
        expectedOutputs.put("key4", false);
        keyValuePairs.put("key5", "");
        expectedOutputs.put("key5", false);

        TieredCacheLoader<String, String> loader = getTieredCacheLoaderWithKeyValueMap(keyValuePairs);

        for (String key : keyValuePairs.keySet()) {
            Boolean expectedOutput = expectedOutputs.get(key);
            String value = spilloverStrategyService.computeIfAbsent(key, loader);
            assertEquals(keyValuePairs.get(key), value);
            String result = spilloverStrategyService.get(key);
            if (expectedOutput) {
                // Should retrieve from disk tier if it was accepted
                assertEquals(keyValuePairs.get(key), result);
            } else {
                // Should miss as heap tier size = 0 and the policy rejected it
                assertNull(result);
            }
        }
    }

    private static class AllowFirstLetterA implements CacheTierPolicy<String> {
        @Override
        public boolean checkData(String data) {
            try {
                return (data.charAt(0) == 'a');
            } catch (StringIndexOutOfBoundsException e) {
                return false;
            }
        }
        @Override
        public boolean isActive() {
            return true;
        }
        @Override
        public void activate() {}
        @Override
        public void deactivate() {}
    }

    private static class AllowEvenLengths implements CacheTierPolicy<String> {
        @Override
        public boolean checkData(String data) {
            return data.length() % 2 == 0;
        }
        @Override
        public boolean isActive() {
            return true;
        }
        @Override
        public void activate() {}
        @Override
        public void deactivate() {}
    }

    private TieredCacheLoader<String, String> getTieredCacheLoader() {
        return new TieredCacheLoader<String, String>() {
            boolean isLoaded = false;

            @Override
            public String load(String key) {
                isLoaded = true;
                return UUID.randomUUID().toString();
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }

    private TieredCacheLoader<String, String> getTieredCacheLoaderWithKeyValueMap(Map<String, String> map) {
        return new TieredCacheLoader<String, String>() {
            boolean isLoaded;
            @Override
            public String load(String key) throws Exception {
                isLoaded = true;
                return map.get(key);
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }

    private TieredCacheSpilloverStrategyService<String, String, String> intializeTieredCacheService(
        int onHeapCacheSize,
        int diskCacheSize,
        TieredCacheEventListener<String, String> cacheEventListener,
        Function<String, String> transformationFunction, // If passed null, default to identity function
        List<CacheTierPolicy<String>> policies // If passed null, default to no policies (empty list)
    ) {
        DiskCachingTier<String, String> diskCache = new MockDiskCachingTier<>(diskCacheSize);
        OnHeapCachingTier<String, String> openSearchOnHeapCache = new MockOnHeapCacheTier<>(onHeapCacheSize);

        Function<String, String> function;
        if (transformationFunction != null) {
            function = transformationFunction;
        } else {
            function = (String value) -> value;
        }

        List<CacheTierPolicy<String>> policiesToUse = new ArrayList<>();
        if (policies != null) {
            policiesToUse = policies;
        }


        return new TieredCacheSpilloverStrategyService.Builder<String, String, String>().setOnHeapCachingTier(openSearchOnHeapCache)
            .setOnDiskCachingTier(diskCache)
            .setTieredCacheEventListener(cacheEventListener)
            .withPreDiskCachingPolicyFunction(function)
            .withPolicies(policiesToUse)
            .build();
    }

    class MockOnHeapCacheTier<K, V> implements OnHeapCachingTier<K, V>, RemovalListener<K, V> {

        Map<K, V> onHeapCacheTier;
        int maxSize;
        private RemovalListener<K, V> removalListener;

        MockOnHeapCacheTier(int size) {
            maxSize = size;
            this.onHeapCacheTier = new ConcurrentHashMap<K, V>();
        }

        @Override
        public CacheValue<V> get(K key) {
            return new CacheValue(this.onHeapCacheTier.get(key), TierType.ON_HEAP, new OnHeapTierRequestStats());
        }

        @Override
        public void put(K key, V value) {
            this.onHeapCacheTier.put(key, value);
        }

        @Override
        public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
            if (this.onHeapCacheTier.size() > maxSize) { // If it exceeds, just notify for evict.
                onRemoval(new RemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, TierType.ON_HEAP));
                return loader.load(key);
            }
            return this.onHeapCacheTier.computeIfAbsent(key, k -> {
                try {
                    return loader.load(key);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public void invalidate(K key) {
            this.onHeapCacheTier.remove(key);
        }

        @Override
        public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
            if (this.onHeapCacheTier.size() >= maxSize) { // If it exceeds, just notify for evict.
                onRemoval(new RemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, TierType.ON_HEAP));
                return loader.load(key);
            }
            return this.onHeapCacheTier.compute(key, ((k, v) -> {
                try {
                    return loader.load(key);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        @Override
        public void setRemovalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = removalListener;
        }

        @Override
        public void invalidateAll() {
            this.onHeapCacheTier.clear();
        }

        @Override
        public Iterable<K> keys() {
            return this.onHeapCacheTier.keySet();
        }

        @Override
        public int count() {
            return this.onHeapCacheTier.size();
        }

        @Override
        public TierType getTierType() {
            return TierType.ON_HEAP;
        }

        @Override
        public void onRemoval(RemovalNotification<K, V> notification) {
            removalListener.onRemoval(notification);
        }
    }

    class MockTieredCacheEventListener<K, V> implements TieredCacheEventListener<K, V> {

        EnumMap<TierType, TestStatsHolder> enumMap = new EnumMap<>(TierType.class);

        MockTieredCacheEventListener() {
            for (TierType tierType : TierType.values()) {
                enumMap.put(tierType, new TestStatsHolder());
            }
        }

        @Override
        public void onMiss(K key, CacheValue<V> cacheValue) {
            enumMap.get(cacheValue.getSource()).missCount.inc();
        }

        @Override
        public void onRemoval(RemovalNotification<K, V> notification) {
            if (notification.getRemovalReason().equals(RemovalReason.EVICTED)) {
                enumMap.get(notification.getTierType()).evictionsMetric.inc();
            }
        }

        @Override
        public void onHit(K key, CacheValue<V> cacheValue) {
            enumMap.get(cacheValue.getSource()).hitCount.inc();
        }

        @Override
        public void onCached(K key, V value, TierType tierType) {
            enumMap.get(tierType).cachedCount.inc();
        }

        class TestStatsHolder {
            final CounterMetric evictionsMetric = new CounterMetric();
            final CounterMetric hitCount = new CounterMetric();
            final CounterMetric missCount = new CounterMetric();

            final CounterMetric cachedCount = new CounterMetric();
        }
    }

    class MockDiskCachingTier<K, V> implements DiskCachingTier<K, V>, RemovalListener<K, V> {
        Map<K, V> diskTier;
        private RemovalListener<K, V> removalListener;
        int maxSize;

        MockDiskCachingTier(int size) {
            this.maxSize = size;
            diskTier = new ConcurrentHashMap<K, V>();
        }

        @Override
        public CacheValue<V> get(K key) {
            return new CacheValue<>(this.diskTier.get(key), TierType.DISK, new DiskTierRequestStats(0L, true));
        }

        @Override
        public void put(K key, V value) {
            if (this.diskTier.size() >= maxSize) { // For simplification
                onRemoval(new RemovalNotification<>(key, value, RemovalReason.EVICTED, TierType.DISK));
                return;
            }
            this.diskTier.put(key, value);
        }

        @Override
        public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
            return this.diskTier.computeIfAbsent(key, k -> {
                try {
                    return loader.load(k);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public void invalidate(K key) {
            this.diskTier.remove(key);
        }

        @Override
        public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
            if (this.diskTier.size() >= maxSize) { // If it exceeds, just notify for evict.
                onRemoval(new RemovalNotification<>(key, loader.load(key), RemovalReason.EVICTED, TierType.DISK));
                return loader.load(key);
            }
            return this.diskTier.compute(key, (k, v) -> {
                try {
                    return loader.load(key);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public void setRemovalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = removalListener;
        }

        @Override
        public void invalidateAll() {
            this.diskTier.clear();
        }

        @Override
        public Iterable<K> keys() {
            return null;
        }

        @Override
        public int count() {
            return this.diskTier.size();
        }

        @Override
        public TierType getTierType() {
            return TierType.DISK;
        }

        @Override
        public void onRemoval(RemovalNotification<K, V> notification) {
            this.removalListener.onRemoval(notification);
        }

        @Override
        public void close() {}
    }
}
