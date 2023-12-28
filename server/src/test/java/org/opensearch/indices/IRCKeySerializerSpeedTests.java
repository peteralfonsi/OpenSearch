/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.Randomness;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class for crudely testing speed differences between the two Key serializers.
 * Should not be merged into main, only for testing during development.
 */
public class IRCKeySerializerSpeedTests extends OpenSearchSingleNodeTestCase {
    public void testSpeed() throws Exception {
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService, dummyClusterSettings);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        Random rand = Randomness.get();

        IRCKeyKryoSerializer kryoSer = new IRCKeyKryoSerializer(indicesService, irc);
        IRCKeyWriteableSerializer writeableSer = new IRCKeyWriteableSerializer(irc);

        int NUM_ITER = 1_000_000;
        int KEY_SIZE = 2048;

        long kryoSerializeTimeNanos = 0L;
        long writeableSerializeTimeNanos = 0L;
        long kryoDeserializeTimeNanos = 0L;
        long writeableDeserializeTimeNanos = 0L;
        for (int i = 0; i < NUM_ITER; i++) {
            IndicesRequestCache.Key key = IRCKeyWriteableSerializerTests.getRandomIRCKey(KEY_SIZE, rand, irc, entity);
            long now = System.nanoTime();
            byte[] kryoSerialized = kryoSer.serialize(key);
            kryoSerializeTimeNanos += System.nanoTime() - now;
            now = System.nanoTime();
            IndicesRequestCache.Key kryoDeserialized = kryoSer.deserialize(kryoSerialized);
            kryoDeserializeTimeNanos += System.nanoTime() - now;
            now = System.nanoTime();
            byte[] writeableSerialized = writeableSer.serialize(key);
            writeableSerializeTimeNanos += System.nanoTime() - now;
            now = System.nanoTime();
            IndicesRequestCache.Key writeableDeserialized = writeableSer.deserialize(writeableSerialized);
            writeableDeserializeTimeNanos += System.nanoTime() - now;
        }
        System.out.println("SERIALIZATION:");
        System.out.println("Kryo total time = " + kryoSerializeTimeNanos + " ns");
        System.out.println("Writeable total time = " + writeableSerializeTimeNanos + " ns");
        System.out.println("Kryo took " + (double) kryoSerializeTimeNanos / writeableSerializeTimeNanos * 100 + "% of writeable");
        System.out.println("DESERIALIZATION:");
        System.out.println("Kryo total time = " + kryoDeserializeTimeNanos + " ns");
        System.out.println("Writeable total time = " + writeableDeserializeTimeNanos + " ns");
        System.out.println("Kryo took " + (double) kryoDeserializeTimeNanos / writeableDeserializeTimeNanos * 100 + "% of writeable");
    }

    public void testSpeedConcurrency() throws Exception {
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService, dummyClusterSettings);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        Random rand = Randomness.get();

        IRCKeyKryoSerializer kryoSer = new IRCKeyKryoSerializer(indicesService, irc);
        IRCKeyWriteableSerializer writeableSer = new IRCKeyWriteableSerializer(irc);

        int numThreads = 32; // total threads, 16 for each serializer
        int totalKeys = 65_536; // total keys for each serializer to process, should be a multiple of numThreads
        int keysPerThread = 2 * totalKeys / (numThreads);
        int valueLength = 2048;

        AtomicLong kryoSerializeTimeNanos = new AtomicLong(0L);
        AtomicLong writeableSerializeTimeNanos = new AtomicLong(0L);
        long kryoDeserializeTimeNanos = 0L;
        long writeableDeserializeTimeNanos = 0L;

        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        CountDownLatch countDownLatch = new CountDownLatch(keysPerThread * numThreads); // different in case it doesnt divide cleanly
        Map<IndicesRequestCache.Key, byte[]> kryoSerializedMap = new HashMap<>();
        Map<IndicesRequestCache.Key, byte[]> writeableSerializedMap = new HashMap<>();
        IndicesRequestCache.Key[] keyArr = new IndicesRequestCache.Key[totalKeys];

        for (int i = 0; i < totalKeys; i++) {
            keyArr[i] = IRCKeyWriteableSerializerTests.getRandomIRCKey(valueLength, rand, irc, entity);
        }

        for (int j = 0; j < numThreads; j += 2) {
            // set up thread for Kryo serializer
            int threadCounter = j / 2;
            threads[j] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                for (int i = threadCounter * keysPerThread; i < (threadCounter +1) * keysPerThread; i++) {
                    IndicesRequestCache.Key key = keyArr[i];
                    long now = System.nanoTime();
                    byte[] ser = kryoSer.serialize(key);
                    kryoSerializeTimeNanos.addAndGet(System.nanoTime() - now);
                    kryoSerializedMap.put(key, ser);
                    countDownLatch.countDown();
                }
            });

            // set up thread for Writeable serializer
            threads[j + 1] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                for (int i = threadCounter * keysPerThread; i < (threadCounter +1) * keysPerThread; i++) {
                    IndicesRequestCache.Key key = keyArr[i];
                    long now = System.nanoTime();
                    byte[] ser = writeableSer.serialize(key);
                    writeableSerializeTimeNanos.addAndGet(System.nanoTime() - now);
                    writeableSerializedMap.put(key, ser);
                    countDownLatch.countDown();
                }
            });

            threads[j].start();
            threads[j + 1].start();
        }
        barrier.await(); // Start the threads in parallel
        countDownLatch.await(); // Wait for all keys in all threads to finish

        System.out.println("SERIALIZATION:");
        System.out.println("Kryo total time = " + kryoSerializeTimeNanos.get() + " ns");
        System.out.println("Writeable total time = " + writeableSerializeTimeNanos.get() + " ns");
        System.out.println("Kryo took " + (double) kryoSerializeTimeNanos.get() / writeableSerializeTimeNanos.get() * 100 + "% of writeable");
        /*System.out.println("DESERIALIZATION:");
        System.out.println("Kryo total time = " + kryoDeserializeTimeNanos + " ns");
        System.out.println("Writeable total time = " + writeableDeserializeTimeNanos + " ns");
        System.out.println("Kryo took " + (double) kryoDeserializeTimeNanos / writeableDeserializeTimeNanos * 100 + "% of writeable");*/
    }
}
