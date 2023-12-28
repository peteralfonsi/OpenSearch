/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.indices;

import org.opensearch.common.Randomness;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Random;
import java.util.UUID;

public class IRCKeyWriteableSerializerTests extends OpenSearchSingleNodeTestCase {

    public void testSerializer() throws Exception {
        ClusterSettings dummyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndicesRequestCache irc = new IndicesRequestCache(Settings.EMPTY, indicesService, dummyClusterSettings);
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(indexShard);
        IRCKeyWriteableSerializer ser = new IRCKeyWriteableSerializer(irc);

        int NUM_KEYS = 1000;
        int[] valueLengths = new int[] { 1000, 6000 }; // test both branches in equals()
        Random rand = Randomness.get();
        for (int valueLength : valueLengths) {
            for (int i = 0; i < NUM_KEYS; i++) {
                IndicesRequestCache.Key key = getRandomIRCKey(valueLength, rand, irc, entity);
                byte[] serialized = ser.serialize(key);
                assertTrue(ser.equals(key, serialized));
                IndicesRequestCache.Key deserialized = ser.deserialize(serialized);
                assertTrue(key.equals(deserialized));
            }
        }
    }

    static IndicesRequestCache.Key getRandomIRCKey(
        int valueLength,
        Random random,
        IndicesRequestCache irc,
        IndicesService.IndexShardCacheEntity entity
    ) {
        byte[] value = new byte[valueLength];
        for (int i = 0; i < valueLength; i++) {
            value[i] = (byte) (random.nextInt(126 - 32) + 32);
        }
        BytesReference keyValue = new BytesArray(value);
        return irc.new Key(entity, keyValue, UUID.randomUUID().toString()); // same UUID source as used in real key
    }
}
