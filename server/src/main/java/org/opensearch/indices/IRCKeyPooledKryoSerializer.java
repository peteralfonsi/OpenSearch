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
import org.opensearch.common.Randomness;
import org.opensearch.common.cache.tier.Serializer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A serializer which maintains multiple component Kryo serializers. Incoming requests are  routed
 * to one of these, and must wait for its synchronized serialize/deserialize methods to become available.
 */
public class IRCKeyPooledKryoSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {

    private final IndicesService indicesService;
    private final IndicesRequestCache irc;
    private final int INITIAL_OUTPUT_BUFFER_SIZE = 1024; // The default size of the Output buffer, but it will grow if needed
    private final int numComponentSerializers; // The number of Kryo objects to maintain
    // TODO: Should this be AtomicBoolean or is the ConcurrentMap enough?

    private final ComponentKryoSerializer[] componentSerializers;
    private AtomicInteger nextComponentSerializerToAssign;


    public IRCKeyPooledKryoSerializer(IndicesService indicesService, IndicesRequestCache irc, int numComponentSerializers) {
        this.indicesService = indicesService;
        this.irc = irc;
        assert numComponentSerializers >= 1;
        this.numComponentSerializers = numComponentSerializers;
        this.componentSerializers = new ComponentKryoSerializer[numComponentSerializers];
        for (int i = 0; i < numComponentSerializers; i++) {
            componentSerializers[i] = new ComponentKryoSerializer();
        }
        this.nextComponentSerializerToAssign = new AtomicInteger(0);
    }

    /**
     * Returns a component serializer to use. To pick, we just cycle through them.
     */
    private ComponentKryoSerializer assignComponentSerializer() {
        int index = nextComponentSerializerToAssign.get();
        int nextIndex = index + 1;
        if (nextIndex >= numComponentSerializers) {
            nextIndex = 0;
        }
        nextComponentSerializerToAssign.set(nextIndex);
        return componentSerializers[index];
    }

    @Override
    public byte[] serialize(IndicesRequestCache.Key object) {
        ComponentKryoSerializer componentSerializer = assignComponentSerializer();
        return componentSerializer.serialize(object);
    }

    @Override
    public IndicesRequestCache.Key deserialize(byte[] bytes) {
        ComponentKryoSerializer componentSerializer = assignComponentSerializer();
        return componentSerializer.deserialize(bytes);
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        IndicesRequestCache.Key deserialized = deserialize(bytes); // Deserialization is ~30% faster than serialization
        return deserialized.equals(object);
    }

    /**
     * An individual serializer with synchronized serialize and deserialize methods.
     */
     class ComponentKryoSerializer {
        private Kryo kryo;
        private Output output;
        private Input input;
        public ComponentKryoSerializer() {
            this.kryo = new Kryo();
            kryo.register(byte[].class);
            this.output = new Output(INITIAL_OUTPUT_BUFFER_SIZE, -1);
            this.input = new Input();
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
