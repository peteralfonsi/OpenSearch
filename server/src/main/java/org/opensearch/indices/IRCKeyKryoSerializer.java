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

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class IRCKeyKryoSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {
    private final int INITIAL_OUTPUT_BUFFER_SIZE = 1024; // The default size of the Output buffer, but it will grow if needed
    private final IndicesService indicesService;
    private final IndicesRequestCache irc;

    private Kryo kryo = null;
    private final Output output;
    private final Input input;

    public IRCKeyKryoSerializer(IndicesService indicesService, IndicesRequestCache irc) {
        this.indicesService = indicesService;
        this.irc = irc;
        this.kryo = getNewKryo();
        this.output = getNewOutput();
        this.input = getNewInput();
        // Kryo classes are not threadsafe. Right now, we just make serializer fns synchronized. In future, may want to create a fixed pool of them
    }

    private Kryo getNewKryo() {
        Kryo kryo = new Kryo();

        // see https://github.com/EsotericSoftware/kryo/issues/885
        kryo.register(String.class);
        kryo.register(byte[].class);
        return kryo;
    }

    private Output getNewOutput() {
        return new Output(INITIAL_OUTPUT_BUFFER_SIZE, -1);
    }

    private Input getNewInput() {
        return new Input();
    }

    private void checkNullKryo() {
        // It's unclear why, but if we try to instantiate the Kryo during setup, it fails with error
        // java.lang.NoClassDefFoundError: org/objenesis/strategy/InstantiatorStrategy
        // If we instead instantiate after the cluster is running, when the Kryo is first needed, there's no error
        // :(
        if (this.kryo == null) {
            this.kryo = getNewKryo();
        }
    }
    @Override
    public synchronized byte[] serialize(IndicesRequestCache.Key object) {
        //Kryo kryo = getNewKryo();
        //Output output = getNewOutput();
        //checkNullKryo();
        output.setBuffer(new byte[INITIAL_OUTPUT_BUFFER_SIZE], -1); // Should this go here??
        // Duplicate what we need to write in writeTo of Key, so we dont serialize the whole IndicesService

        // write necessary info for IndexShardCacheEntity
        assert object.entity instanceof IndicesService.IndexShardCacheEntity;
        IndicesService.IndexShardCacheEntity entity = (IndicesService.IndexShardCacheEntity) object.entity;
        //kryo.writeObject(output, entity.getIndexShard().shardId().getIndex());
        Index index = entity.getIndexShard().shardId().getIndex();
        // Cannot reconstruct Index (no zero-arg constructor), so write its two fields instead

        kryo.writeObject(output, index.getName());
        kryo.writeObject(output, index.getUUID());
        //kryo.writeObject(output, entity.getIndexShard().shardId().id());
        output.writeInt(entity.getIndexShard().shardId().id());

        // write readerCacheKeyId
        kryo.writeObject(output, object.readerCacheKeyId);

        // write only the byte[] within the ByteReference value
        kryo.writeObject(output, BytesReference.toBytes(object.value));

        return output.getBuffer();
    }

    @Override
    public synchronized IndicesRequestCache.Key deserialize(byte[] bytes) {
        //Kryo kryo = getNewKryo();
        //Input input = getNewInput();
        //checkNullKryo();
        input.setBuffer(bytes);
        String indexName = kryo.readObject(input, String.class);
        String indexUUID = kryo.readObject(input, String.class);
        Index index = new Index(indexName, indexUUID);
        int shardIdValue = input.readInt();
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(index, shardIdValue);
        String readerCacheKeyId = kryo.readObject(input, String.class); //input.readString();
        byte[] valueArr = kryo.readObject(input, byte[].class);
        BytesReference value = BytesReference.fromByteBuffer(ByteBuffer.wrap(valueArr));
        return irc.new Key(entity, value, readerCacheKeyId);
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        byte[] serialized = serialize(object);
        return Arrays.equals(serialized, bytes); // Does this work? Check
    }
}
