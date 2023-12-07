/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.tier.keystore.RBMIntKeyLookupStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

public class EhCacheDiskCachingTierTests extends OpenSearchSingleNodeTestCase {

    private static final int CACHE_SIZE_IN_BYTES = 1024 * 101;
    private static final String SETTING_PREFIX = "indices.request.cache";

    public void testBasicGetAndPut() throws IOException {
        HashSet<Setting<?>> clusterSettingsSet = new HashSet<>();
        for (Setting<?> s : ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TIERED_CACHING))) {
            clusterSettingsSet.add(s);
        } // :(
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings =  new ClusterSettings(Settings.EMPTY, clusterSettingsSet);//new ClusterSettings(Settings.EMPTY, Set.of(RBMIntKeyLookupStore.INDICES_CACHE_KEYSTORE_SIZE))
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, String> ehCacheDiskCachingTierNew = new EhCacheDiskCachingTier.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setClusterSettings(clusterSettings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .build();
            int randomKeys = 700; //randomIntBetween(10, 100);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                CacheValue<String> value = ehCacheDiskCachingTierNew.get(entry.getKey());
                assertEquals(entry.getValue(), value.value);
                assertEquals(TierType.DISK, value.getSource());
                assertTrue(((DiskTierRequestStats) value.getStats()).getRequestReachedDisk());
                assertTrue(((DiskTierRequestStats) value.getStats()).getRequestGetTimeNanos() > 0);
            }
            ehCacheDiskCachingTierNew.close();
        }
    }

    public void testBasicGetAndPutBytesReference() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, BytesReference> ehCacheDiskCachingTier = new EhCacheDiskCachingTier.Builder<
                String,
                BytesReference>().setKeyType(String.class)
                .setValueType(BytesReference.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES * 2) // bigger so no evictions happen
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new BytesReferenceSerializer())
                .build();
            int randomKeys = randomIntBetween(10, 100);
            int valueLength = 1000;
            Random rand = Randomness.get();
            Map<String, BytesReference> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                byte[] valueBytes = new byte[valueLength];
                rand.nextBytes(valueBytes);
                keyValueMap.put(UUID.randomUUID().toString(), new BytesArray(valueBytes));
            }
            for (Map.Entry<String, BytesReference> entry : keyValueMap.entrySet()) {
                ehCacheDiskCachingTier.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, BytesReference> entry : keyValueMap.entrySet()) {
                BytesReference value = ehCacheDiskCachingTier.get(entry.getKey()).value;
                assertEquals(entry.getValue(), value);
            }
            ehCacheDiskCachingTier.close();
        }
    }

    public void testConcurrentPut() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, String> ehCacheDiskCachingTierNew = new EhCacheDiskCachingTier.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .build();
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<String, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                CacheValue<String> value = ehCacheDiskCachingTierNew.get(entry.getKey());
                assertEquals(entry.getValue(), value.value);
            }
            ehCacheDiskCachingTierNew.close();
        }
    }

    public void testEhcacheParallelGets() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, String> ehCacheDiskCachingTierNew = new EhCacheDiskCachingTier.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setSettingPrefix(SETTING_PREFIX)
                .setIsEventListenerModeSync(true) // For accurate count
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .build();
            ehCacheDiskCachingTierNew.setRemovalListener(removalListener(new AtomicInteger()));
            int randomKeys = randomIntBetween(20, 100);
            Thread[] threads = new Thread[randomKeys];
            Phaser phaser = new Phaser(randomKeys + 1);
            CountDownLatch countDownLatch = new CountDownLatch(randomKeys);
            Map<String, String> keyValueMap = new HashMap<>();
            int j = 0;
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
            }
            assertEquals(keyValueMap.size(), ehCacheDiskCachingTierNew.count());
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                threads[j] = new Thread(() -> {
                    phaser.arriveAndAwaitAdvance();
                    assertEquals(entry.getValue(), ehCacheDiskCachingTierNew.get(entry.getKey()).value);
                    countDownLatch.countDown();
                });
                threads[j].start();
                j++;
            }
            phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
            countDownLatch.await(); // Wait for all threads to finish
            ehCacheDiskCachingTierNew.close();
        }
    }

    public void testEhcacheKeyIterator() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, String> ehCacheDiskCachingTierNew = new EhCacheDiskCachingTier.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setSettingPrefix(SETTING_PREFIX)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .build();

            int randomKeys = randomIntBetween(2, 2);
            Map<String, String> keyValueMap = new HashMap<>();
            for (int i = 0; i < randomKeys; i++) {
                keyValueMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                ehCacheDiskCachingTierNew.put(entry.getKey(), entry.getValue());
            }
            Iterator<String> keys = ehCacheDiskCachingTierNew.keys().iterator();
            int keysCount = 0;
            while (keys.hasNext()) {
                String key = keys.next();
                keysCount++;
                assertNotNull(ehCacheDiskCachingTierNew.get(key));
            }
            assertEquals(keysCount, randomKeys);
            ehCacheDiskCachingTierNew.close();
        }
    }

    public void testCompute() throws Exception {
        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, String> ehCacheDiskCachingTierNew = new EhCacheDiskCachingTier.Builder<String, String>()
                .setKeyType(String.class)
                .setValueType(String.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setSettingPrefix(SETTING_PREFIX)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .setKeySerializer(new StringSerializer())
                .setValueSerializer(new StringSerializer())
                .build();
            // For now it is unsupported.
            assertThrows(
                UnsupportedOperationException.class,
                () -> ehCacheDiskCachingTierNew.compute("dummy", new TieredCacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return "dummy";
                    }

                    @Override
                    public boolean isLoaded() {
                        return false;
                    }
                })
            );
            assertThrows(
                UnsupportedOperationException.class,
                () -> ehCacheDiskCachingTierNew.computeIfAbsent("dummy", new TieredCacheLoader<>() {
                    @Override
                    public String load(String key) {
                        return "dummy";
                    }

                    @Override
                    public boolean isLoaded() {
                        return false;
                    }
                })
            );
        }
    }

    /*public void testThresholdPolicy() throws Exception {
        long slowTookTimeNanos = 10000000000L; // 10 seconds
        BytesReference slowResult = DiskTierTookTimePolicyTests.getQSRBytesReference(slowTookTimeNanos);

        long fastTookTimeNanos = 100000L; // 100 microseconds
        BytesReference fastResult = DiskTierTookTimePolicyTests.getQSRBytesReference(fastTookTimeNanos);

        long thresholdMillis = 10;

        // For this unit test, set the policy's threshold directly rather than from cluster settings
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskTierTookTimePolicy policy = new DiskTierTookTimePolicy(Settings.EMPTY, dummyClusterSettings);
        policy.setThreshold(new TimeValue(thresholdMillis));

        Settings settings = Settings.builder().build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            EhCacheDiskCachingTier<String, BytesReference> tier = new EhCacheDiskCachingTier.Builder<String, BytesReference>()
                .setKeyType(String.class)
                .setValueType(BytesReference.class)
                .setExpireAfterAccess(TimeValue.MAX_VALUE)
                .setSettings(settings)
                .setThreadPoolAlias("ehcacheTest")
                .setMaximumWeightInBytes(CACHE_SIZE_IN_BYTES)
                .setSettingPrefix(SETTING_PREFIX)
                .setStoragePath(env.nodePaths()[0].indicesPath.toString() + "/request_cache")
                .withPolicy(policy)
                .build();
            tier.put("slow", slowResult);
            assertEquals(slowResult, tier.get("slow")); // key "slow" is found because the policy accepted it
            tier.put("fast", fastResult);
            assertNull(tier.get("fast")); // key "fast" -> null because the policy rejected it
            tier.close();
        }
    }*/

    private RemovalListener<String, String> removalListener(AtomicInteger counter) {
        return notification -> counter.incrementAndGet();
    }

    private static class StringSerializer implements Serializer<String, byte[]> {

        private final Charset charset = StandardCharsets.UTF_8;

        @Override
        public byte[] serialize(String object) {
            return object.getBytes(charset);
        }

        @Override
        public String deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new String(bytes, charset);
        }

        @Override
        public boolean equals(String object, byte[] bytes) {
            return object.equals(deserialize(bytes));
        }
    }
}
