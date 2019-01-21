/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.policy;

import org.mule.runtime.core.api.policy.Policy;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;

/**
 * Default implementation for {@link OperationPolicyProcessorFactory}.
 *
 * @since 4.0
 */
public class DefaultOperationPolicyProcessorFactory implements OperationPolicyProcessorFactory {

  private final PolicyNextChaining policyNextChaining;

  /**
   * Creates a new {@link Processor} from an operation {@link Policy}.
   *
   * @param policyNextChaining the object in charge of hooking the corresponding target for the {@code execute-next} processor.
   */
  public DefaultOperationPolicyProcessorFactory(PolicyNextChaining policyNextChaining) {
    this.policyNextChaining = policyNextChaining;
  }

  @Override
  public ReactiveProcessor createOperationPolicy(Policy policy, ReactiveProcessor nextProcessor) {
    return new OperationPolicyProcessor(policy, policyNextChaining, nextProcessor);
  }
}
