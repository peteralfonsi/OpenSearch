/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.serializer;

import org.opensearch.common.Randomness;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Random;

public class BytesReferenceSerializerTests extends OpenSearchTestCase {
    public void testEquality() throws Exception {
        BytesReferenceSerializer ser = new BytesReferenceSerializer();
        // Test that values are equal before and after serialization, for each implementation of BytesReference.
        int originalSize = 200;
        byte[] bytesValue = new byte[originalSize];
        Random rand = Randomness.get();
        rand.nextBytes(bytesValue);

        BytesReference ba = new BytesArray(bytesValue);
        byte[] serialized = ser.serialize(ba);
        assertTrue(ser.equals(ba, serialized));
        assertEquals(originalSize+1, serialized.length);
        BytesReference deserialized = ser.deserialize(serialized);
        assertEquals(ba, deserialized);
        assertEquals(ba.ramBytesUsed(), deserialized.ramBytesUsed());

        ba = new BytesArray(new byte[] {});
        serialized = ser.serialize(ba);
        assertTrue(ser.equals(ba, serialized));
        assertEquals(1, serialized.length);
        deserialized = ser.deserialize(serialized);
        assertEquals(ba, deserialized);
        assertEquals(ba.ramBytesUsed(), deserialized.ramBytesUsed());

        BytesReference cbr = CompositeBytesReference.of(new BytesArray(bytesValue), new BytesArray(bytesValue));
        serialized = ser.serialize(cbr);
        assertTrue(ser.equals(cbr, serialized));
        //assertEquals(2*originalSize+1, serialized.length);
        deserialized = ser.deserialize(serialized);
        //assertEquals(cbr, deserialized);
        assertEquals(cbr.ramBytesUsed(), deserialized.ramBytesUsed());

        // We need the PagedBytesReference to be larger than the page size (16 KB) in order to actually create it
        byte[] pbrValue = new byte[PageCacheRecycler.PAGE_SIZE_IN_BYTES * 2];
        rand.nextBytes(pbrValue);
        ByteArray arr = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(pbrValue.length);
        arr.set(0L, pbrValue, 0, pbrValue.length);
        assert !arr.hasArray();
        BytesReference pbr = BytesReference.fromByteArray(arr, pbrValue.length);
        serialized = ser.serialize(pbr);
        assertTrue(ser.equals(pbr, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(pbr, deserialized);
        assertEquals(pbr.ramBytesUsed(), deserialized.ramBytesUsed());

        BytesReference rbr = new ReleasableBytesReference(new BytesArray(bytesValue), ReleasableBytesReference.NO_OP);
        serialized = ser.serialize(rbr);
        assertTrue(ser.equals(rbr, serialized));
        deserialized = ser.deserialize(serialized);
        assertEquals(rbr, deserialized);
        assertEquals(rbr.ramBytesUsed(), deserialized.ramBytesUsed());
    }
}
