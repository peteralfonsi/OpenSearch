/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.serializer;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.Arrays;

/**
 * A serializer which transforms BytesReference to byte[].
 * The type of BytesReference is NOT preserved after deserialization, but nothing in opensearch should care.
 */
public class BytesReferenceSerializer implements Serializer<BytesReference, byte[]> {
    // This class does not get passed to ehcache itself, so it's not required that classes match after deserialization.

    private final static byte PADDING_BYTE = 0x7f;

    public BytesReferenceSerializer() {}

    @Override
    public byte[] serialize(BytesReference object) {
        //
        int originalSize = (int) object.ramBytesUsed();
        byte[] result = Arrays.copyOfRange(BytesReference.toBytes(object), 0, originalSize+1);
        result[originalSize] = PADDING_BYTE; // Add an additional nonzero byte to ensure trailing zeros aren't dropped
        return result;
    }

    @Override
    public BytesReference deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        byte[] unpadded = Arrays.copyOfRange(bytes, 0, bytes.length-1); // Discard nonzero padding byte
        assert unpadded.length == bytes.length - 1;
        return new BytesArray(unpadded);
    }

    @Override
    public boolean equals(BytesReference object, byte[] bytes) {
        return Arrays.equals(serialize(object), bytes);
    }
}
