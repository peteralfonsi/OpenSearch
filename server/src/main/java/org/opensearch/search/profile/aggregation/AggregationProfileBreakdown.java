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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.search.profile.aggregation;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ProfileMetric;
import org.opensearch.search.profile.ProfileMetricUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableMap;

/**
 * {@linkplain AbstractProfileBreakdown} customized to work with aggregations.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AggregationProfileBreakdown extends AbstractProfileBreakdown {
    private final Map<String, Object> extra = new HashMap<>();

    public AggregationProfileBreakdown() {
        this(ProfileMetricUtil.getAggregationProfileMetrics());
    }

    public AggregationProfileBreakdown(Collection<Supplier<ProfileMetric>> timers) {
        super(timers);
    }

    /**
     * Add extra debugging information about the aggregation.
     */
    public void addDebugInfo(String key, Object value) {
        extra.put(key, value);
    }

    @Override
    public Map<String, Object> toDebugMap() {
        return unmodifiableMap(extra);
    }

}
