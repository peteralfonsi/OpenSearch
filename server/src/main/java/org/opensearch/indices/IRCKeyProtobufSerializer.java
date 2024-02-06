/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.opensearch.common.cache.tier.Serializer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;

import java.nio.ByteBuffer;

public class IRCKeyProtobufSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {

    private final IndicesService indicesService;
    private final IndicesRequestCache irc;

    public IRCKeyProtobufSerializer(IndicesService indicesService, IndicesRequestCache irc) {
        this.indicesService = indicesService;
        this.irc = irc;
    }
    @Override
    public byte[] serialize(IndicesRequestCache.Key object) {
        assert object.entity instanceof IndicesService.IndexShardCacheEntity;
        IndicesService.IndexShardCacheEntity entity = (IndicesService.IndexShardCacheEntity) object.entity;
        Index index = entity.getIndexShard().shardId().getIndex();
        return IRCKeyValues.Values.newBuilder()
            .setIndexName(index.getName())
            .setIndexUuid(index.getUUID())
            .setShardId(entity.getIndexShard().shardId().id())
            .setReaderCacheKeyId(object.readerCacheKeyId)
            .setValue(ByteString.copyFrom(BytesReference.toBytes(object.value)))
            .build()
            .toByteArray();
    }

    @Override
    public IndicesRequestCache.Key deserialize(byte[] bytes){
        IRCKeyValues.Values values = null;
        try {
            values = IRCKeyValues.Values.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        String indexName = values.getIndexName();
        String indexUUID = values.getIndexUuid();
        int shardIdValue = values.getShardId();
        String readerCacheKeyId = values.getReaderCacheKeyId();
        byte[] valueArr = values.getValue().toByteArray();

        Index index = new Index(indexName, indexUUID);
        IndicesService.IndexShardCacheEntity entity = indicesService.new IndexShardCacheEntity(index, shardIdValue);
        BytesReference value = BytesReference.fromByteBuffer(ByteBuffer.wrap(valueArr));
        return irc.new Key(entity, value, readerCacheKeyId);
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        IndicesRequestCache.Key deserialized = deserialize(bytes);
        return deserialized.equals(object);
    }
}
