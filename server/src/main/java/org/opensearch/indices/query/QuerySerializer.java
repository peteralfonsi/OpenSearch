/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.query;

import org.apache.lucene.document.LatLonPoint;
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

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;

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
    static final Query geoBoundingBoxQuery = LatLonPoint.newBoxQuery("", 0.0, 0.0, 0.0, 0.0);

    // Bytes for different type of PointRangeQuery
    static final byte LONG_POINT_BYTE = 0x01;
    static final byte GEO_BOUNDING_BOX_BYTE = 0x02;

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
        final byte pointRangeTypeByte = getPointRangeTypeByte(prQuery);
        os.writeByte(pointRangeTypeByte);
        switch (pointRangeTypeByte) {
            case LONG_POINT_BYTE:
                serializeLongPoint(os, prQuery);
                break;
            case GEO_BOUNDING_BOX_BYTE:
                serializeGeoBoundingBox(os, prQuery);
                break;
        }
    }

    private void serializeLongPoint(BytesStreamOutput os, PointRangeQuery prQuery) throws IOException {
        os.writeString(prQuery.getField());

        byte[] lowerPoint = prQuery.getLowerPoint();
        os.writeVInt(lowerPoint.length);
        os.writeBytes(lowerPoint);

        byte[] upperPoint = prQuery.getUpperPoint();
        os.writeVInt(upperPoint.length);
        os.writeBytes(upperPoint);
    }

    private Query deserializeLongPoint(BytesStreamInput is) throws IOException {
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

    private void serializeGeoBoundingBox(BytesStreamOutput os, PointRangeQuery prQuery) throws IOException {
        // Serializes queries produced via LatLonPoint.newBoxQuery()
        // TODO: For now, assume the query does NOT cross the dateline. If it does, newBoxQuery() does something complex, so avoid this case
        // for now.
        // I think in this case the query type ends up being ConstantScoreQuery, so it's probably going to fail on its own anyway.

        os.writeString(prQuery.getField());

        byte[] lowerPoint = prQuery.getLowerPoint();
        assert lowerPoint.length == 2 * Integer.BYTES;
        os.writeBytes(lowerPoint);

        byte[] upperPoint = prQuery.getUpperPoint();
        assert upperPoint.length == 2 * Integer.BYTES;
        os.writeBytes(upperPoint);
    }

    private Query deserializeGeoBoundingBox(BytesStreamInput is) throws IOException {
        String field = is.readString();
        byte[] values = new byte[4 * Integer.BYTES];
        is.readBytes(values, 0, 4 * Integer.BYTES);
        double minLatitude = decodeLatitude(values, 0);
        double minLongitude = decodeLongitude(values, Integer.BYTES);
        double maxLatitude = decodeLatitude(values, 2 * Integer.BYTES);
        double maxLongitude = decodeLongitude(values, 3 * Integer.BYTES);

        return LatLonPoint.newBoxQuery(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }

    private Query deserializePointRangeQuery(BytesStreamInput is) throws IOException {
        final byte pointRangeTypeByte = is.readByte();
        switch (pointRangeTypeByte) {
            case LONG_POINT_BYTE:
                return deserializeLongPoint(is);
            case GEO_BOUNDING_BOX_BYTE:
                return deserializeGeoBoundingBox(is);
        }
        throw new UnsupportedOperationException("Unrecognized PointRangeQuery type byte" + pointRangeTypeByte);
    }

    private void serializeDummyQuery(BytesStreamOutput os, Query query) throws IOException {
        os.writeInt(((DummyQuery) query).getId());
    }

    private Query deserializeDummyQuery(BytesStreamInput is) throws IOException {
        return new DummyQuery(is.readInt());
    }

    private byte getPointRangeTypeByte(PointRangeQuery query) {
        if (isLongPointRangeQuery(query)) return LONG_POINT_BYTE;
        if (isGeoBoundingBoxQuery(query)) return GEO_BOUNDING_BOX_BYTE;
        throw new UnsupportedOperationException("Cannot serialize this type of PointRangeQuery");
    }

    private boolean isLongPointRangeQuery(PointRangeQuery query) {
        // TODO: A gross hack - but ok for the PoC
        return query.getClass() == longPointRangeQuery.getClass();
    }

    private boolean isGeoBoundingBoxQuery(PointRangeQuery query) {
        return query.getClass() == geoBoundingBoxQuery.getClass();
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
