/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.HashMap;

public class RestClearIndicesCacheActionTests extends OpenSearchTestCase {
    public void testRequestCacheSet() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("request", "true");
        final RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        ClearIndicesCacheRequest cacheRequest = RestClearIndicesCacheAction.fromRequest(restRequest, new ClearIndicesCacheRequest());
        assertTrue(cacheRequest.requestCache());
    }

    public void testRequestCacheOnDiskSet() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("requestCacheOnDisk", "true");
        final RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        ClearIndicesCacheRequest cacheRequest = RestClearIndicesCacheAction.fromRequest(restRequest, new ClearIndicesCacheRequest());
        assertTrue(cacheRequest.requestCacheOnDisk());
    }

    public void testRequestCacheOnHeapSet() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("requestCacheOnHeap", "true");
        final RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        ClearIndicesCacheRequest cacheRequest = RestClearIndicesCacheAction.fromRequest(restRequest, new ClearIndicesCacheRequest());
        assertTrue(cacheRequest.requestCacheOnHeap());
    }

    public void testParseClearScrollRequestWithInvalidJsonThrowsException() {
        HashMap<String, String> request_params = new HashMap<>();
        request_params.put("request", "true");
        request_params.put("requestCacheOnHeap", "true");

        RestClearIndicesCacheAction action = new RestClearIndicesCacheAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(request_params).build();
        Exception e = expectThrows(IOException.class, () -> action.prepareRequest(request, null));
        assertEquals(e.getMessage(), "Invalid input");
        assertEquals(e.getCause().getMessage(), "Invalid parameters: cannot have both requestCache and requestCacheOnHeap set to true");
    }
}
