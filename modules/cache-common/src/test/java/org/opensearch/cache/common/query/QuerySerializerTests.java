/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.test.OpenSearchTestCase;

public class QuerySerializerTests extends OpenSearchTestCase {
    public void testLongPointRangeQuery() throws Exception {
        String field = "date";
        long[] lowerPoint = new long[] { 0, 5, 199, 28238 };
        long[] upperPoint = new long[] { 1, 688, 200, 30000 };

        Query original = LongPoint.newRangeQuery(field, lowerPoint, upperPoint);
        QuerySerializer serializer = new QuerySerializer();
        byte[] ser = serializer.serialize(original);
        Query deser = serializer.deserialize(ser);
        assertTrue(serializer.equals(original, ser));
        assertEquals(original, deser);
    }

    public void testOtherPointRangeQueriesFail() throws Exception {
        String field = "date";
        double[] lowerPoint = new double[] { 0, 5, 199, 28238 };
        double[] upperPoint = new double[] { 1, 688, 200, 30000 };

        Query original = DoublePoint.newRangeQuery(field, lowerPoint, upperPoint);
        QuerySerializer serializer = new QuerySerializer();
        assertThrows(UnsupportedOperationException.class, () -> serializer.serialize(original));
    }

    public void testDummyQuery() throws Exception {
        int id = 7;
        Query original = new DummyQuery(id);
        QuerySerializer serializer = new QuerySerializer();
        byte[] ser = serializer.serialize(original);
        Query deser = serializer.deserialize(ser);
        assertTrue(serializer.equals(original, ser));
        assertEquals(original, deser);
        assertNotEquals(new DummyQuery(id - 1), original);
        assertNotEquals(new DummyQuery(id - 1), deser);
    }

    public void testGeoBoundingBoxQuery() throws Exception {
        String field = "location";
        double minLatitude = -4.9;
        double minLongitude = -122.0;
        double maxLatitude = 44.0;
        double maxLongitude = -121.9;

        Query original = LatLonPoint.newBoxQuery(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
        QuerySerializer serializer = new QuerySerializer();
        byte[] ser = serializer.serialize(original);
        Query deser = serializer.deserialize(ser);
        assertTrue(serializer.equals(original, ser));
        assertEquals(original, deser);
    }
}
