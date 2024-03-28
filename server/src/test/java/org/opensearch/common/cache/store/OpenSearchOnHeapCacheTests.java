/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class OpenSearchOnHeapCacheTests extends OpenSearchTestCase {
    private final static long keyValueSize = 50;
    private final static List<String> dimensionNames = List.of("dim1", "dim2", "dim3");

    public void testStats() throws Exception {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = between(10, 50);
        int numEvicted = between(10, 20);
        OpenSearchOnHeapCache<String, String> cache = getCache(maxKeys, listener);

        List<ICacheKey<String>> keysAdded = new ArrayList<>();
        int numAdded = maxKeys + numEvicted;
        for (int i = 0; i < numAdded; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            keysAdded.add(key);
            cache.computeIfAbsent(key, getLoadAwareCacheLoader());

            assertEquals(i + 1, cache.stats().getTotalMisses());
            assertEquals(0, cache.stats().getTotalHits());
            assertEquals(Math.min(maxKeys, i + 1), cache.stats().getTotalEntries());
            assertEquals(Math.min(maxKeys, i + 1) * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(Math.max(0, i + 1 - maxKeys), cache.stats().getTotalEvictions());
        }
        // do gets from the last part of the list, which should be hits
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.computeIfAbsent(keysAdded.get(i), getLoadAwareCacheLoader());
            int numHits = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(numHits, cache.stats().getTotalHits());
            assertEquals(maxKeys, cache.stats().getTotalEntries());
            assertEquals(maxKeys * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }

        // invalidate keys
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.invalidate(keysAdded.get(i));
            int numInvalidated = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(maxKeys, cache.stats().getTotalHits());
            assertEquals(maxKeys - numInvalidated, cache.stats().getTotalEntries());
            assertEquals((maxKeys - numInvalidated) * keyValueSize, cache.stats().getTotalSizeInBytes());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }
    }

    private OpenSearchOnHeapCache<String, String> getCache(int maxSizeKeys, MockRemovalListener<String, String> listener) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        Settings settings = Settings.builder()
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                maxSizeKeys * keyValueSize + "b"
            )
            .build();

        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setValueType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(listener)
            .setSettings(settings)
            .setDimensionNames(dimensionNames)
            .setMaxSizeInBytes(maxSizeKeys * keyValueSize)
            .build();
        return (OpenSearchOnHeapCache<String, String>) onHeapCacheFactory.create(cacheConfig, CacheType.INDICES_REQUEST_CACHE, null);
    }

    public void testInvalidateWithDropDimensions() throws Exception {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = 50;
        OpenSearchOnHeapCache<String, String> cache = getCache(maxKeys, listener);

        List<ICacheKey<String>> keysAdded = new ArrayList<>();

        for (int i = 0; i < maxKeys - 5; i++) {
            ICacheKey<String> key = new ICacheKey<>(UUID.randomUUID().toString(), getRandomDimensions());
            keysAdded.add(key);
            cache.computeIfAbsent(key, getLoadAwareCacheLoader());
        }

        ICacheKey<String> keyToDrop = keysAdded.get(0);

        Map<String, Object> xContentMap = getStatsXContentMap(cache.stats(), dimensionNames);
        List<String> xContentMapKeys = getXContentMapKeys(keyToDrop);
        Map<String, Object> individualSnapshotMap = (Map<String, Object>) MultiDimensionCacheStatsTests.getValueFromNestedXContentMap(
            xContentMap,
            xContentMapKeys
        );
        assertNotNull(individualSnapshotMap);
        assertEquals(5, individualSnapshotMap.size()); // Assert all 5 stats are present and not null
        for (Map.Entry<String, Object> entry : individualSnapshotMap.entrySet()) {
            Integer value = (Integer) entry.getValue();
            assertNotNull(value);
        }

        for (CacheStatsDimension dim : keyToDrop.dimensions) {
            dim.setDropStatsOnInvalidation(true);
        }
        cache.invalidate(keyToDrop);

        // Now assert the stats are gone for any key that has this combination of dimensions, but still there otherwise
        xContentMap = getStatsXContentMap(cache.stats(), dimensionNames);
        for (ICacheKey<String> keyAdded : keysAdded) {
            xContentMapKeys = getXContentMapKeys(keyAdded);
            individualSnapshotMap = (Map<String, Object>) MultiDimensionCacheStatsTests.getValueFromNestedXContentMap(
                xContentMap,
                xContentMapKeys
            );
            if (keyAdded.dimensions.equals(keyToDrop.dimensions)) {
                assertNull(individualSnapshotMap);
            } else {
                assertNotNull(individualSnapshotMap);
            }
        }
    }

    private List<String> getXContentMapKeys(ICacheKey<?> iCacheKey) {
        List<String> result = new ArrayList<>();
        for (CacheStatsDimension dim : iCacheKey.dimensions) {
            result.add(dim.dimensionName);
            result.add(dim.dimensionValue);
        }
        return result;
    }

    private List<CacheStatsDimension> getRandomDimensions() {
        Random rand = Randomness.get();
        int bound = 3;
        List<CacheStatsDimension> result = new ArrayList<>();
        for (String dimName : dimensionNames) {
            result.add(new CacheStatsDimension(dimName, String.valueOf(rand.nextInt(bound))));
        }
        return result;
    }

    private static class MockRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {
        CounterMetric numRemovals;

        MockRemovalListener() {
            numRemovals = new CounterMetric();
        }

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            numRemovals.inc();
        }
    }

    // Public as this is used in other tests as well
    public static Map<String, Object> getStatsXContentMap(CacheStats cacheStats, List<String> levels) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        Map<String, String> paramMap = Map.of("level", String.join(",", levels));
        ToXContent.Params params = new ToXContent.MapParams(paramMap);

        builder.startObject();
        cacheStats.toXContent(builder, params);
        builder.endObject();

        String resultString = builder.toString();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), resultString, true);
    }

    private ICacheKey<String> getICacheKey(String key) {
        List<CacheStatsDimension> dims = new ArrayList<>();
        for (String dimName : dimensionNames) {
            dims.add(new CacheStatsDimension(dimName, "0"));
        }
        return new ICacheKey<>(key, dims);
    }

    private LoadAwareCacheLoader<ICacheKey<String>, String> getLoadAwareCacheLoader() {
        return new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public String load(ICacheKey<String> key) {
                isLoaded = true;
                return UUID.randomUUID().toString();
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }
}
