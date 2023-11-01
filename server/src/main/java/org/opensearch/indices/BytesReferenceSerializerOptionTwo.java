/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Arrays;

public class BytesReferenceSerializerOptionTwo implements Serializer<BytesReference, byte[]> {
    // This class does not get passed to ehcache itself, so it's not required that classes match after deserialization.

    public BytesReferenceSerializerOptionTwo() {}
    @Override
    public byte[] serialize(BytesReference object) throws IOException {
        return BytesReference.toBytes(object);
    }

    @Override
    public BytesReference deserialize(byte[] bytes) throws IOException {
        return new BytesArray(bytes);
    }

    @Override
    public boolean equals(BytesReference object, byte[] bytes) throws IOException {
        return Arrays.equals(serialize(object), bytes);
    }
}
