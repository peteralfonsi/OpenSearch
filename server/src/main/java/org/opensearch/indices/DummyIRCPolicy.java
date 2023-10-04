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

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * A dummy cache policy used for testing.
 * Always accepts or rejects queries based on doAccept value passed to constructor.
 */
public class DummyIRCPolicy implements CacheTierPolicy<QuerySearchResult> {
    private final boolean doAccept;
    private final String name;
    public DummyIRCPolicy(boolean doAccept, String policyName) {
        this.doAccept = doAccept;
        this.name = policyName;
    }

    protected String buildDeniedString() {
        return String.format(
            "Dummy policy %s rejects this query",
            name
        );
    }
    @Override
    public CheckDataResult checkData(BytesReference data) throws IOException {
        if (doAccept) {
            return new CheckDataResult(true, null);
        }
        return new CheckDataResult(false, buildDeniedString());
    }
}
