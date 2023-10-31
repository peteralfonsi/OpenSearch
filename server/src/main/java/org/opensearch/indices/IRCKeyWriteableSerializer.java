/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class serializes the IndicesRequestCache.Key using its writeTo method.
 */
public class IRCKeyWriteableSerializer implements Serializer<IndicesRequestCache.Key, ByteBuffer> {

    IndicesRequestCache irc;
    public IRCKeyWriteableSerializer(IndicesRequestCache irc) {
        this.irc = irc;
    }
    @Override
    public ByteBuffer serialize(IndicesRequestCache.Key object) throws IOException {
        BytesStreamOutput os = new BytesStreamOutput();
        object.writeTo(os);
        return ByteBuffer.wrap(BytesReference.toBytes(os.bytes()));
    }

    @Override
    public IndicesRequestCache.Key deserialize(ByteBuffer bytes) throws IOException {
        byte[] out = new byte[bytes.remaining()];
        bytes.get(out);
        BytesStreamInput is = new BytesStreamInput();
        is.readBytes(out, 0, out.length);
        return irc.new Key(is);
    }

    @Override
    public boolean equals(IndicesRequestCache.Key object, ByteBuffer bytes) throws IOException {
        // This will probably be used to check for keys in the ehcache?
        // If so, performance probably matters a good bit
        // Maybe we could change which order we do it in based on key size?

        // Deserialization is much slower than serialization for keys of order 1 KB,
        // while time to serialize is fairly constant (per byte)
        if (bytes.remaining() < 5000) {
            return serialize(object).equals(bytes);
        } else {
            return object.equals(deserialize(bytes));
        }
    }
}
