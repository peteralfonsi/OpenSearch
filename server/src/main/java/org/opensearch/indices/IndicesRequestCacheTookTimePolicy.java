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

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

public class IndicesRequestCacheTookTimePolicy implements CacheTierPolicy<QuerySearchResult> {
    public static final Setting<TimeValue> INDEX_REQUEST_CACHE_DISK_TIMETOOK_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "index.requests.cache.disk.tooktime.threshold",
        new TimeValue(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
        // I think node scope is appropriate, because disk seek time probably depends on node hardware?
        // Or did Sagar mean something else by "cluster-level", I can't find a cluster scope in Setting.java
    );

    private final TimeValue threshold; // is this correct to be final if it's dynamic? How can we change this in the policy as someone changes it with the API?

    public IndicesRequestCacheTookTimePolicy(Settings settings) {
        this.threshold = INDEX_REQUEST_CACHE_DISK_TIMETOOK_THRESHOLD_SETTING.get(settings);
    }

    protected String buildDeniedString(TimeValue tookTime, TimeValue threshold) {
        // separating out for use in testing
        return String.format(
            "Query took time %d ms is less than threshold value %d ms",
            tookTime.getMillis(),
            threshold.getMillis()
        );
    }
    @Override
    public CheckDataResult checkData(BytesReference data) throws IOException {
        // maybe check value of setting here, each time?
        QuerySearchResult qsr = new QuerySearchResult(data.streamInput()); // may throw IOException
        TimeValue tookTime = TimeValue.timeValueNanos(qsr.getTookTimeNanos());
        boolean isAccepted = true;
        String deniedReason = null;
        if (tookTime.compareTo(threshold) < 0) { // negative -> tookTime is shorter than threshold
            isAccepted = false;
            deniedReason = buildDeniedString(tookTime, threshold);
        }
        return new CheckDataResult(isAccepted, deniedReason);
    }
}
