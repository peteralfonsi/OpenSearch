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
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.opensearch.common.settings.Setting;

public class ConfigurableBKDLeafSizeCodec extends FilterCodec {

    public static final String CONFIGURABLE_BKD_LEAF_SIZE_CODEC_NAME = "ConfigurableBKDLeafSizeCodec";

    public static final String BKD_MAX_POINTS_IN_LEAF_KEY = "bkd.max_leaf_points";

    public static final Setting<Integer> BKD_MAX_POINTS_IN_LEAF_SETTING = Setting.intSetting(
        BKD_MAX_POINTS_IN_LEAF_KEY,
        512,
        Setting.Property.IndexScope
    );

    public ConfigurableBKDLeafSizeCodec() {
        super(CONFIGURABLE_BKD_LEAF_SIZE_CODEC_NAME, new Lucene912Codec());
    }

    @Override
    public final PointsFormat pointsFormat() {
        return new LeafSizePointsFormat();
    }
}
