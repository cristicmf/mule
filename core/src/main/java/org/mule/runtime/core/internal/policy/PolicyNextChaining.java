/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.policy;

import org.mule.runtime.core.api.processor.ReactiveProcessor;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;

/**
 * Helps in the hooking of the {@code execute-next} processor of policies with the actual chain to be executed.
 */
public class PolicyNextChaining {

  private final Map<String, ReactiveProcessor> nextOperationMap =
      Caffeine.newBuilder().weakValues().<String, ReactiveProcessor>build().asMap();

  public void updateNextOperation(String identifier, ReactiveProcessor nextOperation) {
    nextOperationMap.put(identifier, nextOperation);
  }

  public ReactiveProcessor retrieveNextOperation(String identifier) {
    return nextOperationMap.get(identifier);
  }

  public ReactiveProcessor clearNextOperation(String identifier) {
    return nextOperationMap.remove(identifier);
  }

}
