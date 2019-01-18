/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.policy;

import static org.mule.runtime.api.message.Message.of;
import static org.slf4j.LoggerFactory.getLogger;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Mono.just;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.policy.Policy;
import org.mule.runtime.core.api.policy.PolicyChain;
import org.mule.runtime.core.api.policy.PolicyStateHandler;
import org.mule.runtime.core.api.policy.PolicyStateId;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.internal.exception.MessagingException;
import org.mule.runtime.core.internal.message.InternalEvent;
import org.mule.runtime.core.privileged.event.PrivilegedEvent;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;

/**
 * This class is responsible for the processing of a policy applied to a {@link Processor}. Currently the only kind of
 * {@link Processor} supported is an operation from the SDK.
 * <p>
 * In order for this class to be able to execute a policy it requires an {@link PolicyChain} with the content of the policy. Such
 * policy may have an {@link PolicyNextActionMessageProcessor} which will be the one used to execute the provided
 * {@link Processor}.
 * <p>
 * This class enforces the scoping of variables between the actual behaviour and the policy that may be applied to it. To enforce
 * such scoping of variables it uses {@link PolicyStateHandler} so the last {@link CoreEvent} modified by the policy behaviour can
 * be stored and retrieve for later usages. It also uses {@link PolicyEventConverter} as a helper class to convert an
 * {@link CoreEvent} from the policy to the next operation {@link CoreEvent} or from the next operation result to the
 * {@link CoreEvent} that must continue the execution of the policy.
 * <p>
 */
public class OperationPolicyProcessor implements ReactiveProcessor {

  private static final Logger LOGGER = getLogger(OperationPolicyProcessor.class);

  private static final String POLICY_OPERATION_ORIGINAL_EVENT = "policy.operation.originalEvent";

  private final Policy policy;
  private final PolicyStateHandler policyStateHandler;
  private final PolicyNextChaining policyNextChaining;
  private final PolicyEventConverter policyEventConverter = new PolicyEventConverter();
  private final ReactiveProcessor nextProcessor;
  private final PolicyStateIdFactory stateIdFactory;

  // Force the reference to be kept until this processor is GC'd. On 4.1.x, this is when the event is finished.
  private ReactiveProcessor nextOperationCall;

  public OperationPolicyProcessor(Policy policy,
                                  PolicyStateHandler policyStateHandler, PolicyNextChaining policyNextChaining,
                                  ReactiveProcessor nextProcessor) {
    this.policy = policy;
    this.policyStateHandler = policyStateHandler;
    this.policyNextChaining = policyNextChaining;
    this.nextProcessor = nextProcessor;
    this.stateIdFactory = new PolicyStateIdFactory(policy.getPolicyId());
  }

  /**
   * Process the policy chain of processors. The provided {@code nextOperation} function has the behaviour to be executed by the
   * next-operation of the chain.
   *
   * @param operationEvent the event with the data to execute the operation
   * @return the result of processing the {@code event} through the policy chain.
   * @throws MuleException
   */
  @Override
  public Publisher<CoreEvent> apply(Publisher<CoreEvent> publisher) {
    return from(publisher)
        .cast(PrivilegedEvent.class)
        .map(operationEvent -> InternalEvent.builder(operationEvent)
            // TODO use a quickCopy
            .addInternalParameter(POLICY_OPERATION_ORIGINAL_EVENT + policy.getPolicyId(), operationEvent)
            .build())
        .doOnNext(operationEvent -> {

        })
        .flatMap(operationEvent -> {
          PolicyStateId policyStateId = stateIdFactory.create(operationEvent);
          PrivilegedEvent variablesProviderEvent = variablesProvider(operationEvent, policyStateId);
          PrivilegedEvent policyEvent = policyEventConverter.createEvent(operationEvent, variablesProviderEvent);
          nextOperationCall = buildOperationExecutionWithPolicyFunction(policyStateId);
          policyNextChaining.updateNextOperation(policyStateId.getExecutionIdentifier(), nextOperationCall);
          return executePolicyChain(operationEvent, policyStateId, policyEvent);
        });
  }

  private void manageError(PolicyStateId policyStateId, PrivilegedEvent operationEvent, MessagingException messagingException) {
    saveState(policyStateId, (PrivilegedEvent) messagingException.getEvent());
    PrivilegedEvent newEvent = policyEventConverter.createEvent((PrivilegedEvent) messagingException.getEvent(), operationEvent);
    messagingException.setProcessedEvent(newEvent);
  }

  private Mono<PrivilegedEvent> executePolicyChain(PrivilegedEvent operationEvent, PolicyStateId policyStateId,
                                                   PrivilegedEvent policyEvent) {

    PolicyChain policyChain = policy.getPolicyChain();
    policyChain.onChainError(t -> manageError(policyStateId, operationEvent, (MessagingException) t));

    return just(policyEvent)
        .doOnNext(event -> logPolicy(event.getContext().getCorrelationId(), policy.getPolicyId(),
                                     () -> getMessageAttributesAsString(event), "Before operation"))
        .cast(CoreEvent.class)
        .transform(policyChain)
        .cast(PrivilegedEvent.class)
        .doOnNext(policyChainResult -> saveState(policyStateId, policyChainResult))
        .map(policyChainResult -> policyEventConverter.createEvent(policyChainResult, operationEvent))
        .doOnNext(event -> logPolicy(event.getContext().getCorrelationId(), policy.getPolicyId(),
                                     () -> getMessageAttributesAsString(event), "After operation"));
  }

  private ReactiveProcessor buildOperationExecutionWithPolicyFunction(PolicyStateId policyStateId) {
    return publisher -> from(publisher)
        .map(event -> (CoreEvent) policyEventConverter.createEvent(saveState(policyStateId, (PrivilegedEvent) event),
                                                                   getOriginalEvent(event)))
        .transform(nextProcessor)
        .map(result -> (CoreEvent) policyEventConverter.createEvent((PrivilegedEvent) result, loadState(policyStateId)));
  }

  private PrivilegedEvent getOriginalEvent(CoreEvent event) {
    return ((InternalEvent) event).getInternalParameter(POLICY_OPERATION_ORIGINAL_EVENT + policy.getPolicyId());
  }

  private PrivilegedEvent saveState(PolicyStateId policyStateId, PrivilegedEvent event) {
    policyStateHandler.updateState(policyStateId, event);
    return event;
  }

  private PrivilegedEvent loadState(PolicyStateId policyStateId) {
    return (PrivilegedEvent) policyStateHandler.getLatestState(policyStateId).get();
  }

  private PrivilegedEvent variablesProvider(CoreEvent event, PolicyStateId policyStateId) {
    Optional<CoreEvent> latestPolicyState = policyStateHandler.getLatestState(policyStateId);
    return (PrivilegedEvent) latestPolicyState
        .orElseGet(() -> PrivilegedEvent.builder(event.getContext()).message(of(null)).build());
  }

  private String getMessageAttributesAsString(CoreEvent event) {
    if (event.getMessage() == null || event.getMessage().getAttributes() == null
        || event.getMessage().getAttributes().getValue() == null) {
      return "";
    }
    return event.getMessage().getAttributes().getValue().toString();
  }

  private void logPolicy(String eventId, String policyName, Supplier<String> message, String startingMessage) {
    if (LOGGER.isTraceEnabled()) {
      // TODO Remove event id when first policy generates it. MULE-14455
      LOGGER.trace("Event Id: " + eventId + ".\n" + startingMessage + "\nPolicy:" + policyName + "\n" + message.get());
    }
  }
}
