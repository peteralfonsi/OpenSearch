/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.clear;

import junit.framework.TestCase;

import static org.junit.Assert.assertThrows;

public class ClearIndicesCacheRequestTest extends TestCase {

  public void testValidateInputCacheAndOnHeap() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCache(true);
    request.requestCacheOnHeap(true);
    Exception e = assertThrows(IllegalArgumentException.class, request::validateInput);
    assertTrue(e.getMessage().contains("Invalid parameters: cannot have both requestCache and requestCacheOnHeap set to true"));
  }

  public void testValidateInputCacheAndOnDisk() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCache(true);
    request.requestCacheOnDisk(true);
    Exception e = assertThrows(IllegalArgumentException.class, request::validateInput);
    assertTrue(e.getMessage().contains("Invalid parameters: cannot have both requestCache and requestCacheOnDisk set to true"));
  }

  public void testValidateInputCacheOnHeapAndOnDisk() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCacheOnHeap(true);
    request.requestCacheOnDisk(true);
    request.validateInput();
  }

  public void testValidateInputCacheOnHeap() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCacheOnHeap(true);
    request.validateInput();
  }

  public void testValidateInputCacheOnDisk() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCacheOnDisk(true);
    request.validateInput();
  }
  public void testValidateInputClearCache() {
    ClearIndicesCacheRequest request = new ClearIndicesCacheRequest();
    request.requestCache(true);
    request.validateInput();
  }
}