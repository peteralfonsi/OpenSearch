/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.Randomness;
import org.opensearch.common.cache.tier.CacheValue;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadPoolExecutor;

public class IRCKeyKryoSerializerTests extends OpenSearchSingleNodeTestCase {
    public void testSerializer() throws Exception {
        int numComponentSerializers = 5;
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService, dummyClusterSettings);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        IRCKeyPooledKryoSerializer ser = new IRCKeyPooledKryoSerializer(indicesService, irc, numComponentSerializers);

        int NUM_KEYS = 1000;
        Random rand = Randomness.get();
        int[] valueLengths = new int[] { 512, 1024, 2048}; // test various values above/below default buffer size of 1024
        for (int valueLength : valueLengths) {
            for (int i = 0; i < NUM_KEYS; i++) {
                IndicesRequestCache.Key key = IRCKeyWriteableSerializerTests.getRandomIRCKey(valueLength, rand, irc, entity);
                byte[] serialized = ser.serialize(key);
                assertTrue(ser.equals(key, serialized));
                IndicesRequestCache.Key deserialized = ser.deserialize(serialized);
                assertTrue(key.equals(deserialized));
            }
        }
    }

    public void testConcurrency() throws Exception {
        int numComponentSerializers = 5;
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService, dummyClusterSettings);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        IRCKeyPooledKryoSerializer ser = new IRCKeyPooledKryoSerializer(indicesService, irc, numComponentSerializers);

        Random rand = Randomness.get();

        // Based on EhCacheDiskCachingTierTests.testConcurrentPut()
        int numKeys = 300;
        int valueLength = 2048;

        Thread[] threads = new Thread[numKeys];
        Phaser phaser = new Phaser(numKeys + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numKeys);
        Map<IndicesRequestCache.Key, byte[]> serializedMap = new HashMap<>();
        IndicesRequestCache.Key[] keyArr = new IndicesRequestCache.Key[numKeys];
        int j = 0;
        for (int i = 0; i < numKeys; i++) {
            keyArr[i] = IRCKeyWriteableSerializerTests.getRandomIRCKey(valueLength, rand, irc, entity);
        }
        for (IndicesRequestCache.Key key : keyArr) {
            threads[j] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                serializedMap.put(key, ser.serialize(key));
                countDownLatch.countDown();
            });
            threads[j].start();
            j++;
        }
        phaser.arriveAndAwaitAdvance(); // Will trigger parallel puts above.
        countDownLatch.await(); // Wait for all threads to finish
        for (Map.Entry<IndicesRequestCache.Key, byte[]> entry : serializedMap.entrySet()) {
            assertTrue(ser.equals(entry.getKey(), entry.getValue()));
        }

    }

}
