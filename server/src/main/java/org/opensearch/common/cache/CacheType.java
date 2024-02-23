/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashSet;
import java.util.Set;

/**
 * Cache types available within OpenSearch.
 */
@ExperimentalApi
public enum CacheType {
    INDICES_REQUEST_CACHE("indices.requests.cache", "request_cache");
    private final String settingPrefix;
    private final String representation; // For use in API

    CacheType(String settingPrefix, String representation) {
        this.settingPrefix = settingPrefix;
        this.representation = representation;
    }

    public String getSettingPrefix() {
        return settingPrefix;
    }
    public String getRepresentation() {
        return representation;
    }

    public static CacheType getByRepresentation(String representation) {
        for (CacheType cacheType : values()) {
            if (cacheType.representation.equals(representation)) {
                return cacheType;
            }
        }
        throw new IllegalArgumentException("No CacheType with representation = " + representation);
    }

    public static Set<String> allRepresentations() {
        Set<String> reprs = new HashSet<>();
        for (CacheType cacheType : values()) {
            reprs.add(cacheType.representation);
        }
        return reprs;
    }
}
