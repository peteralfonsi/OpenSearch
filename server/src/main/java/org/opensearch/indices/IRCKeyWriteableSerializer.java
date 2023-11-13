/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.common.cache.tier.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class serializes the IndicesRequestCache.Key using its writeTo method.
 */
public class IRCKeyWriteableSerializer implements Serializer<IndicesRequestCache.Key, byte[]> {

    IndicesRequestCache irc;
    public IRCKeyWriteableSerializer(IndicesRequestCache irc) {
        this.irc = irc;
    }
    @Override
    public byte[] serialize(IndicesRequestCache.Key object) {
        try {
            BytesStreamOutput os = new BytesStreamOutput();
            object.writeTo(os);
            return BytesReference.toBytes(os.bytes());
        } catch (IOException e) {
            throw new OpenSearchException(e);
        }
    }

    @Override
    public IndicesRequestCache.Key deserialize(byte[] bytes) {
        try {
            BytesStreamInput is = new BytesStreamInput(bytes, 0, bytes.length);
            return irc.new Key(is);
        } catch (IOException e) {
            throw new OpenSearchException(e);
        }
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, byte[] bytes) {
        // Deserialization is much slower than serialization for keys of order 1 KB,
        // while time to serialize is fairly constant (per byte)
        if (bytes.length < 5000) {
            return Arrays.equals(serialize(object), bytes);
        } else {
            return object.equals(deserialize(bytes));
        }
    }
}
