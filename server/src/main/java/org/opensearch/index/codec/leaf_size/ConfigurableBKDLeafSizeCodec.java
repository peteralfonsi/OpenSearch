/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.leaf_size;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.bkd.BKDWriter;
import org.opensearch.common.settings.Setting;

import java.io.IOException;

public class ConfigurableBKDLeafSizeCodec extends FilterCodec {

    public static final String CONFIGURABLE_BKD_LEAF_SIZE_CODEC_NAME = "ConfigurableBKDLeafSizeCodec";

    public static final String BKD_MAX_POINTS_IN_LEAF_KEY = "bkd.max_leaf_points";

    public static final Setting<Integer> BKD_MAX_POINTS_IN_LEAF_SETTING = Setting.intSetting(
        BKD_MAX_POINTS_IN_LEAF_KEY,
        512,
        Setting.Property.NodeScope
    );

    private final int maxPointsInLeafNode;

    public ConfigurableBKDLeafSizeCodec(int maxPointsInLeafNode) {
        super(CONFIGURABLE_BKD_LEAF_SIZE_CODEC_NAME, new Lucene912Codec());
        this.maxPointsInLeafNode = maxPointsInLeafNode;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return new LeafSizePointsFormat(maxPointsInLeafNode);
    }

    public static class LeafSizePointsFormat extends PointsFormat {

        private final int maxPointsInLeafNode;

        public LeafSizePointsFormat(int maxPointsInLeafNode) {
            this.maxPointsInLeafNode = maxPointsInLeafNode;
        }

        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return new Lucene90PointsWriter(state, maxPointsInLeafNode, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) throws IOException {
            return new Lucene90PointsReader(state);
        }
    }
}
