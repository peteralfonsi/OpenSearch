/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.bytes.PagedBytesReference;
import org.opensearch.core.common.util.ByteArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class will be passed to the EhCacheDiskCachingTier(Key, BytesValue) instance.
 * Ehcache requires the class of an object must be the same after deserialization, but
 * BytesReference is an interface, and its implementing classes don't have a constructor in common.
 * Because the disk tier is generic we can't add logic in put() to change incoming BytesReference objects
 * to one class, like BytesArray.
 * So, we must record the class on serialization and manually put in a case for each implementing class.
 */

public class BytesReferenceSerializer implements Serializer<BytesReference, ByteBuffer> {

    // Supported implementations of BytesReference
    private ArrayList<Class<? extends BytesReference>> implTypes = new ArrayList<>(Arrays.asList(
        BytesArray.class,
        CompositeBytesReference.class,
        PagedBytesReference.class,
        ReleasableBytesReference.class
    ));

    @Override
    public ByteBuffer serialize(BytesReference object) throws IOException, SerializationException {
        //BytesReferenceImpl bytesReferenceType;
        Class<? extends BytesReference> clazz = object.getClass();
        int index = implTypes.indexOf(clazz);
        if (index < 0) {
            throw new SerializationException("BytesReference type " + clazz + " not yet supported by BytesReferenceSerializer");
        }
        byte[] objectBytes = BytesReference.toBytes(object);
        byte[] serialized = new byte[objectBytes.length + 1];
        // We don't control the creation of byte[] from the BytesReference so unfortunately we have to copy into new larger array
        serialized[0] = (byte) index;
        System.arraycopy(objectBytes, 0, serialized, 1, objectBytes.length);
        return ByteBuffer.wrap(serialized);
    }

    @Override
    public BytesReference deserialize(ByteBuffer bytes) throws IOException {
        byte[] out = new byte[bytes.remaining()];
        bytes.get(out);
        Class<? extends BytesReference> clazz = implTypes.get(out[0]);
        return BytesReferenceSerializer.fromByteArray(out, 1, out.length - 1, clazz);
    }

    @Override
    public boolean equals(BytesReference object, ByteBuffer bytes) throws IOException {
        return false;
    }

    // compare this impl to just making BytesReference Java serializable to see how bad time hit is
    // also check how serialization libraries do their perf testing - batching? key size?

    /**
     * This function is used during deserialization in BytesReferenceSerializer to ensure
     * a deserialized BytesReference object has the same class as the original object.
     * @param bytes The bytes to use to create the object.
     * @param clazz Which implementing class to use.
     * @return
     */
    static BytesReference fromByteArray(byte[] bytes, int offset, int length, Class<? extends BytesReference> clazz) throws SerializationException {
        if (clazz == BytesArray.class) {
            return new BytesArray(bytes, offset, length);
        } else if (clazz == CompositeBytesReference.class) {
            return CompositeBytesReference.of(new BytesArray(bytes, offset, length));
        } else if (clazz == PagedBytesReference.class) {
            ByteArray arr = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(length);
            arr.set(0L, bytes, offset, length);
            assert arr.hasArray();
            return BytesReference.fromByteArray(arr, length);// Returns PagedByteArray if arr.hasArray()
        } else if (clazz == ReleasableBytesReference.class) {
            return ReleasableBytesReference.wrap(new BytesArray(bytes, offset, length));
        }
        throw new SerializationException("BytesReference type " + clazz + " not yet supported by BytesReferenceSerializer");
    }
}
