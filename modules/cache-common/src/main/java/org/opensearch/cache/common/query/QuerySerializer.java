/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Map;

public class QuerySerializer implements Serializer<Query, byte[]> {
    // TODO - will have to basically do one query type at a time. Not TermQuery ever lol.

    static final byte INVALID_QUERY_BYTE = 0x00; // Used for all queries that aren't serializable
    static final byte POINT_RANGE_QUERY_BYTE = 0x01;

    @Override
    public byte[] serialize(Query object) {
        byte classByte = getClassByte(object);
        try {
            BytesStreamOutput os = new BytesStreamOutput();
            os.writeByte(classByte);
            switch (classByte) {
                case POINT_RANGE_QUERY_BYTE:


            }
        } catch (IOException e){ 



        return BytesReference.toBytes(os.bytes());
    }

    @Override
    public Query deserialize(byte[] bytes) {
        return null;
    }

    @Override
    public boolean equals(Query object, byte[] bytes) {
        return false;
    }

    public boolean isAllowed(Query query) {
        // TODO - report true for serializable, false for not serializable. Feed this into a policy into the TSC to control disk tier access.
        return getClassByte(query) != INVALID_QUERY_BYTE;
    }

    byte getClassByte(Query query) {
        if (query instanceof PointRangeQuery) return POINT_RANGE_QUERY_BYTE;
        return INVALID_QUERY_BYTE;
    }
}
