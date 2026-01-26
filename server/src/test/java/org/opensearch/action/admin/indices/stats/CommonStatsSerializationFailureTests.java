/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to\n * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CommonStatsSerializationFailureTests extends OpenSearchTestCase {

    public void testSerializationWithBadFieldDataStats() throws IOException {
        CommonStats commonStats = new CommonStats();
        commonStats.fieldData = new FieldDataStats.Builder().memorySize(-100L).evictions(10L).build();
        commonStats.docs = new DocsStats(100, 50, 1000);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            commonStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                CommonStats deserialized = new CommonStats(in);

                assertNull(deserialized.getFieldData());
                assertNotNull(deserialized.getDocs());
                assertEquals(100, deserialized.getDocs().getCount());
            }
        }
    }

    public void testSerializationWithMultipleBadStats() throws IOException {
        CommonStats commonStats = new CommonStats();
        commonStats.fieldData = new FieldDataStats.Builder().memorySize(-100L).build();
        commonStats.docs = new DocsStats(100, 50, 1000);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            commonStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                CommonStats deserialized = new CommonStats(in);

                assertNull(deserialized.getFieldData());
                assertNotNull(deserialized.getDocs());
            }
        }
    }

    public void testSerializationWithAllGoodStats() throws IOException {
        CommonStats commonStats = new CommonStats();
        commonStats.fieldData = new FieldDataStats.Builder().memorySize(100L).evictions(5L).build();
        commonStats.docs = new DocsStats(100, 50, 1000);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            commonStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                CommonStats deserialized = new CommonStats(in);

                assertNotNull(deserialized.getFieldData());
                assertEquals(100L, deserialized.getFieldData().getMemorySizeInBytes());
                assertNotNull(deserialized.getDocs());
                assertEquals(100, deserialized.getDocs().getCount());
            }
        }
    }

    public void testSerializationWithNullStats() throws IOException {
        CommonStats commonStats = new CommonStats();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            commonStats.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                CommonStats deserialized = new CommonStats(in);

                assertNull(deserialized.getFieldData());
                assertNull(deserialized.getDocs());
            }
        }
    }
}
