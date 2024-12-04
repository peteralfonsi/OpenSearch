/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.test.OpenSearchTestCase;

public class QuerySerializerTests extends OpenSearchTestCase {
    public void testLongPointRangeQuery() throws Exception {
        String field = "date";
        long[] lowerPoint = new long[]{0, 5, 199, 28238};
        long[] upperPoint = new long[]{1, 688, 200, 30000};

        Query original = LongPoint.newRangeQuery(field, lowerPoint, upperPoint);
        QuerySerializer serializer = new QuerySerializer();
        byte[] ser = serializer.serialize(original);
        Query deser = serializer.deserialize(ser);
        assertTrue(serializer.equals(original, ser));
        assertEquals(original, deser);
    }

    public void testOtherPointRangeQueriesFail() throws Exception {
        String field = "date";
        double[] lowerPoint = new double[]{0, 5, 199, 28238};
        double[] upperPoint = new double[]{1, 688, 200, 30000};

        Query original = DoublePoint.newRangeQuery(field, lowerPoint, upperPoint);
        QuerySerializer serializer = new QuerySerializer();
        assertThrows(UnsupportedOperationException.class, () -> serializer.serialize(original));
    }
}
