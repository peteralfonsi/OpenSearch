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

public class BytesArraySerializer implements Serializer<BytesArray> {
    @Override
    public byte[] serialize(BytesArray object) throws IOException {
        return BytesReference.toBytes(object);
    }

    @Override
    public BytesArray deserialize(byte[] bytes) throws IOException {
        return new BytesArray(bytes);
    }

    @Override
    public boolean equals(BytesArray object, byte[] bytes) throws IOException {
        return Arrays.equals(serialize(object), bytes);
    }
}
