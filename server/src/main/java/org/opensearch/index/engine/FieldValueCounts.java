/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proof of concept only: A class that optionally keeps track of counts for field values in a single SHARD, not a segment.
 * Will stop keeping track once a certain cardinality is exceeded for a field.
 */
@ExperimentalApi
public class FieldValueCounts {
    public static final Setting<Integer> MAX_FIELD_VALUE_COUNT_CARDINALITY_SETTING = Setting.intSetting(
        "field_value_count.max_cardinality",
        10_000
    );

    private final Map<String, SortedMap<Object, AtomicLong>> fieldCounts;

    private final Map<String, Boolean> trackingEnabled;
    private final int maxCardinality;

    public FieldValueCounts(Settings settings) {
        this.fieldCounts = new ConcurrentHashMap<>();
        this.trackingEnabled = new ConcurrentHashMap<>();
        this.maxCardinality = MAX_FIELD_VALUE_COUNT_CARDINALITY_SETTING.get(settings);
    }

    protected void incrementCount(String fieldName, Object fieldValue) {
        boolean trackingEnabledForField = trackingEnabled.computeIfAbsent(fieldName, this::isFieldEligibleForTracking);
        if (!(trackingEnabledForField)) {
            return;
        }

        SortedMap<Object, AtomicLong> individualFieldCounts = fieldCounts.computeIfAbsent(fieldName, (f) -> new ConcurrentSkipListMap<>());
        AtomicLong count = individualFieldCounts.computeIfAbsent(fieldValue, (v) -> new AtomicLong());
        count.incrementAndGet();
        checkCardinality(fieldName, individualFieldCounts);
    }

    private boolean isFieldEligibleForTracking(String fieldName) {
        // TODO: This is a crude method, check if this is actually valid!
        return !fieldName.startsWith("_");
    }

    private void checkCardinality(String fieldName, SortedMap<Object, AtomicLong> individualFieldCounts) {
        if (individualFieldCounts.size() > maxCardinality) {
            trackingEnabled.put(fieldName, false);
            // Delete stored values to save space
            fieldCounts.remove(fieldName);
        }
    }

    public SortedMap<Object, AtomicLong> getFieldCounts(String fieldName) {
        if (!(trackingEnabled.getOrDefault(fieldName, false))) {
            return null;
        }
        return fieldCounts.get(fieldName);
    }
}
