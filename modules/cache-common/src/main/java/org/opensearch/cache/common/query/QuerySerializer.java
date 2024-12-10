/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.query;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.serializer.Serializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A class to serialize Query objects. Not all Query objects are supported. You can check with isAllowed().
 */
public class QuerySerializer implements Serializer<Query, byte[]> {
    // TODO - will have to basically do one query type at a time. Not TermQuery ever lol.

    static final byte INVALID_QUERY_BYTE = 0x00; // Used for all queries that aren't serializable
    static final byte POINT_RANGE_QUERY_BYTE = 0x01;
    static final byte DUMMY_QUERY_BYTE = 0x02;

    // TODO: Used as part of gross hack to determine which impl of PointRangeQuery comes in.
    static final Query longPointRangeQuery = LongPoint.newRangeQuery("", 0, 1);

    /**
     * Required for javadocs.
     */
    public QuerySerializer() {}
    @Override
    public byte[] serialize(Query object) {
        if (object == null) return null;
        byte classByte = getClassByte(object);
        BytesStreamOutput os;
        try {
            os = new BytesStreamOutput();
            os.writeByte(classByte);
            switch (classByte) {
                case POINT_RANGE_QUERY_BYTE:
                    serializePointRangeQuery(os, object);
                    break;
                case DUMMY_QUERY_BYTE:
                    serializeDummyQuery(os, object);
                    break;
                /*default:
                throw new UnsupportedOperationException("Invalid class byte");*/
            }
        } catch (IOException e) {
            throw new OpenSearchException("Error serializing query: ", e);
        }
        return BytesReference.toBytes(os.bytes());
    }

    @Override
    public Query deserialize(byte[] bytes) {
        if (bytes == null) return null;
        try {
            BytesStreamInput is = new BytesStreamInput(bytes, 0, bytes.length);
            byte classByte = is.readByte();
            switch (classByte) {
                case POINT_RANGE_QUERY_BYTE:
                    return deserializePointRangeQuery(is);
                case DUMMY_QUERY_BYTE:
                    return deserializeDummyQuery(is);
                /*default:
                    throw new UnsupportedOperationException("Invalid class byte");*/
            }
        } catch (IOException e) {
            throw new OpenSearchException("Error deserializing query: ", e);
        }
        return null;
    }

    @Override
    public boolean equals(Query object, byte[] bytes) {
        return Arrays.equals(serialize(object), bytes);
    }

    private void serializePointRangeQuery(BytesStreamOutput os, Query query) throws IOException {
        PointRangeQuery prQuery = (PointRangeQuery) query;
        // Determine if it's LongPoint's implementation
        if (!isLongPointRangeQuery(prQuery)) {
            throw new UnsupportedOperationException("can only serialize LongPoint's implementation of PointRangeQuery");
        }
        os.writeString(prQuery.getField());

        byte[] lowerPoint = prQuery.getLowerPoint();
        os.writeVInt(lowerPoint.length);
        os.writeBytes(lowerPoint);

        byte[] upperPoint = prQuery.getUpperPoint();
        os.writeVInt(upperPoint.length);
        os.writeBytes(upperPoint);
        // TODO: for PointRangeQuery these are byte[] but for LongPoint's impl they're long[]
        // It looks like LongPoint calls pack() (public) before shoving it into bytes. So I need to do the reverse of pack() on the way
        // out...
    }

    private Query deserializePointRangeQuery(BytesStreamInput is) throws IOException {
        // TODO: Currently ONLY reads LongPoint's implementation of PointRangeQuery
        String field = is.readString();

        int lowerPointLength = is.readVInt();
        byte[] lowerPoint = new byte[lowerPointLength];
        is.readBytes(lowerPoint, 0, lowerPointLength);
        long[] lowerPointLong = new long[lowerPoint.length / 8];
        LongPoint.unpack(new BytesRef(lowerPoint), 0, lowerPointLong);

        int upperPointLength = is.readVInt();
        byte[] upperPoint = new byte[upperPointLength];
        is.readBytes(upperPoint, 0, upperPointLength);
        long[] upperPointLong = new long[upperPoint.length / 8];
        LongPoint.unpack(new BytesRef(upperPoint), 0, upperPointLong);

        return LongPoint.newRangeQuery(field, lowerPointLong, upperPointLong);
    }

    private void serializeDummyQuery(BytesStreamOutput os, Query query) throws IOException {
        os.writeInt(((DummyQuery) query).getId());
    }

    private Query deserializeDummyQuery(BytesStreamInput is) throws IOException {
        return new DummyQuery(is.readInt());
    }

    private boolean isLongPointRangeQuery(PointRangeQuery query) {
        // TODO: A gross hack - but ok for the PoC
        // TODO: also - does it work??
        return query.getClass() == longPointRangeQuery.getClass();
    }

    /**
     * Checks whether the query is serializable.
     * @param query The query to serialize.
     * @return Whether it's serializable.
     */
    public boolean isAllowed(Query query) {
        return getClassByte(query) != INVALID_QUERY_BYTE;
    }

    byte getClassByte(Query query) {
        if (query instanceof PointRangeQuery) return POINT_RANGE_QUERY_BYTE;
        if (query instanceof DummyQuery) return DUMMY_QUERY_BYTE;
        return INVALID_QUERY_BYTE;
    }
}
