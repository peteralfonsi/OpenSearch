/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * A cache tier policy which accepts queries whose took time is greater than some threshold,
 * which is specified as a dynamic cluster-level setting. The threshold should be set to approximately
 * the time it takes to get a result from the cache tier.
 */
public class DiskTierTookTimePolicy implements CacheTierPolicy<QuerySearchResult> {
    public static final Setting<TimeValue> INDICES_REQUEST_CACHE_DISK_TOOKTIME_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "index.requests.cache.disk.tooktime.threshold",
        new TimeValue(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue threshold;

    public DiskTierTookTimePolicy(Settings settings, ClusterSettings clusterSettings) {
        this.threshold = INDICES_REQUEST_CACHE_DISK_TOOKTIME_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(INDICES_REQUEST_CACHE_DISK_TOOKTIME_THRESHOLD_SETTING, this::setThreshold);
    }

    protected void setThreshold(TimeValue threshold) { // public so that we can manually set value in unit test
        this.threshold = threshold;
    }

    @Override
    public QuerySearchResult convertFromBytesReference(BytesReference data) throws IOException {
        try {
            return new QuerySearchResult(data.streamInput());
        } catch (IllegalStateException ise) {
            throw new IOException(ise);
        }
    }

    @Override
    public boolean checkData(BytesReference data) throws IOException {
        QuerySearchResult qsr = convertFromBytesReference(data);
        Long tookTimeNanos = qsr.getTookTimeNanos();
        if (tookTimeNanos == null) {
            return true;
            // Received a null took time -> this QSR is from an old version which does not have took time, we should accept it
        }
        TimeValue tookTime = TimeValue.timeValueNanos(qsr.getTookTimeNanos());
        if (tookTime.compareTo(threshold) < 0) { // negative -> tookTime is shorter than threshold
            return false;
        }
        return true;
    }
}
