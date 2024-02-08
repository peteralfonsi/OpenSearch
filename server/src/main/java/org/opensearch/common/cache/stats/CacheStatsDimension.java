/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

public class CacheStatsDimension {
    public final String dimensionName;
    public final String dimensionValue;
    public CacheStatsDimension(String dimensionName, String dimensionValue) {
        this.dimensionName = dimensionName;
        this.dimensionValue = dimensionValue;
    }
}
