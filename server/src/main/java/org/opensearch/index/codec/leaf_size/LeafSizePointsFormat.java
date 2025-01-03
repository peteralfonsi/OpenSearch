/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.leaf_size;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.bkd.BKDWriter;
import org.opensearch.common.settings.Setting;

import java.io.IOException;

public class LeafSizePointsFormat extends PointsFormat {

    public LeafSizePointsFormat() {}

    @Override
    public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene90PointsWriter(state, 1024, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP); // TODO: Connect this to OpenSearch setting
    }

    @Override
    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene90PointsReader(state);
    }
}
