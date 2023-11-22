/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier.enums;

/**
 * Cache store types in tiered cache.
 */
public enum CacheStoreType {

    ON_HEAP("on_heap"),
    DISK("disk");

    private final String stringValue;

    CacheStoreType(String stringValue) {
        // Associate each TierType with a string representation, for use in API responses and elsewhere
        this.stringValue = stringValue;
    }

    public String getStringValue() {
        return this.stringValue;
    }
}
