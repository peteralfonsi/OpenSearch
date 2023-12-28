/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.opensearch.common.cache.tier.Serializer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A serializer which maintains multiple Kryo objects that can be accessed concurrently.
 */
public class IRCKeyPooledKryoSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {

    private final IndicesService indicesService;
    private final IndicesRequestCache irc;
    private final int INITIAL_OUTPUT_BUFFER_SIZE = 1024; // The default size of the Output buffer, but it will grow if needed
    private final int kryoConcurrency; // The number of Kryo objects to maintain to be used concurrently
    // TODO: Should this be AtomicBoolean or is the ConcurrentMap enough?

    // These maps hold the actual Kryo objects, as well as which ones are available
    private final ConcurrentMap<Integer, KryoObjects> availabilityMap;
    private final long backoffTimeNanos = 50_000L; // 50 microseconds - tweak


    public IRCKeyPooledKryoSerializer(IndicesService indicesService, IndicesRequestCache irc, int kryoConcurrency) {
        this.indicesService = indicesService;
        this.irc = irc;
        assert kryoConcurrency >= 1;
        this.kryoConcurrency = kryoConcurrency;
        this.availabilityMap = new ConcurrentHashMap<>();
        for (int i = 0; i < kryoConcurrency; i++) {
            availabilityMap.put(i, new KryoObjects());
        }
    }

    /**
     * Gets the index of an available Kryo object for serialization or deserialization.
     * Because it's synchronized, only one index will ever try to be obtained at a time.
     * This index will be released once the serialization or deserialization function is done.
     * @return the index
     */
    private synchronized KryoObjects getAvailableKryoObjects() {
        for (int i = 0; i < kryoConcurrency; i++) {
            if (availabilityMap.get(i).getAvailability()) {
                availabilityMap.get(i).setUnavailable();
                return availabilityMap.get(i);
            }
        }
        return null; // If nothing is currently available
    }

    @Override
    public byte[] serialize(IndicesRequestCache.Key object) {
        // This method has to be gated by getAvailableKryoObjects().
        KryoObjects kryoObjects = getAvailableKryoObjects();
        if (kryoObjects == null) {
            // Try again after backoff period
            Thread.sleep()
        }
        return new byte[0];
    }

    @Override
    public IndicesRequestCache.Key deserialize(byte[] bytes) {
        return null;
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        return false;
    }

    /**
     * A wrapper class for a Kryo, Output, and Input that are all used together.
     */
     class KryoObjects {
        private Kryo kryo;
        private Output output;
        private Input input;
        private AtomicBoolean isAvailable;
        //private final IndicesService indicesService;
        //private final IndicesRequestCache irc;
        public KryoObjects() {
            //this.indicesService = indicesService;
            //this.irc = irc;
            this.kryo = new Kryo();
            kryo.register(byte[].class);
            this.output = new Output(INITIAL_OUTPUT_BUFFER_SIZE, -1);
            this.input = new Input();
            this.isAvailable = new AtomicBoolean(true);
        }
        public boolean getAvailability() {
            return isAvailable.get();
        }

        public void setUnavailable() {
            isAvailable.set(false);
        }

        public synchronized byte[] serialize(IndicesRequestCache.Key object) {
            output.setBuffer(new byte[INITIAL_OUTPUT_BUFFER_SIZE], -1);
            // Duplicate what we need to write in writeTo of Key, so we dont serialize the whole IndicesService

            // write necessary info for IndexShardCacheEntity
            assert object.entity instanceof IndicesService.IndexShardCacheEntity;
            IndicesService.IndexShardCacheEntity entity = (IndicesService.IndexShardCacheEntity) object.entity;
            //kryo.writeObject(output, entity.getIndexShard().shardId().getIndex());
            Index index = entity.getIndexShard().shardId().getIndex();
            // Cannot reconstruct Index (no zero-arg constructor), so write its two fields instead
            output.writeString(index.getName());
            output.writeString(index.getUUID());
            output.writeInt(entity.getIndexShard().shardId().id());

            // write readerCacheKeyId
            output.writeString(object.readerCacheKeyId);

            // write only the byte[] within the ByteReference value
            kryo.writeObject(output, BytesReference.toBytes(object.value));

            return output.getBuffer();
        }

        public synchronized IndicesRequestCache.Key deserialize(byte[] bytes) {
            input.setBuffer(bytes);
            String indexName = input.readString();
            String indexUUID = input.readString();
            Index index = new Index(indexName, indexUUID);
            int shardIdValue = input.readInt();
            IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(index, shardIdValue);
            String readerCacheKeyId = input.readString();
            byte[] valueArr = kryo.readObject(input, byte[].class);
            BytesReference value = BytesReference.fromByteBuffer(ByteBuffer.wrap(valueArr));
            return irc.new Key(entity, value, readerCacheKeyId);
        }


    }
}
