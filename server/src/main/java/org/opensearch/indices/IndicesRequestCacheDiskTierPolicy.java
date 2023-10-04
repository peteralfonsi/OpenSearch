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

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * This policy takes in an array containing an instance of all policies we want to apply to the IRC disk tier.
 * It short circuits these policies' checks together, in the provided order, to get one overall check.
 */
public class IndicesRequestCacheDiskTierPolicy implements CacheTierPolicy<QuerySearchResult> {
    private final CacheTierPolicy<QuerySearchResult>[] policies;
    private final int numPolicies;
    private final boolean allowedByDefault;
    public static String DEFAULT_DENIED_REASON = "No policies were supplied to IndicesRequestCacheDiskTierPolicy and allowedByDefault = false";
    // available here for testing purposes

    public IndicesRequestCacheDiskTierPolicy(CacheTierPolicy<QuerySearchResult>[] policies, boolean allowedByDefault) {
        this.policies = policies;
        this.numPolicies = policies.length;
        this.allowedByDefault = allowedByDefault; // default behavior if no other policies are supplied
    }
    @Override
    public CheckDataResult checkData(BytesReference data) throws IOException {
        if (numPolicies == 0) {
            // still need to check the data can be deserialized into QuerySearchResult
            QuerySearchResult qsr;
            try {
                qsr = new QuerySearchResult(data.streamInput());
            } catch (IllegalStateException ise) {
                throw new IOException(ise);
            }

            if (allowedByDefault) {
                return new CheckDataResult(true, null);
            } else {
                return new CheckDataResult(
                    false, DEFAULT_DENIED_REASON
                );
            }
        }
        CheckDataResult result = policies[0].checkData(data);
        if (numPolicies > 1) {
            for (int i = 1; i < numPolicies; i++) {
                result = result.composeWith(policies[i].checkData(data));
                if (!result.isAccepted()) {
                    break;
                }
            }
        }
        return result;
    }
}
