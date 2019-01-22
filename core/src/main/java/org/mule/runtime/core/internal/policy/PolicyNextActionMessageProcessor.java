/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.runtime.core.internal.policy;

import static java.util.Collections.singletonMap;
import static org.mule.runtime.api.notification.PolicyNotification.AFTER_NEXT;
import static org.mule.runtime.api.notification.PolicyNotification.BEFORE_NEXT;
import static org.mule.runtime.core.internal.event.EventQuickCopy.quickCopy;
import static org.mule.runtime.core.internal.policy.OperationPolicyProcessor.POLICY_OPERATION_ORIGINAL_EVENT;
import static org.mule.runtime.core.internal.policy.SourcePolicyProcessor.POLICY_SOURCE_ORIGINAL_EVENT;
import static org.mule.runtime.core.privileged.processor.MessageProcessors.processToApply;
import static org.slf4j.LoggerFactory.getLogger;
import static reactor.core.publisher.Flux.from;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Mono.subscriberContext;

import org.mule.runtime.api.component.AbstractComponent;
import org.mule.runtime.api.component.location.ComponentLocation;
import org.mule.runtime.api.event.EventContext;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.lifecycle.Initialisable;
import org.mule.runtime.api.lifecycle.InitialisationException;
import org.mule.runtime.api.message.Message;
import org.mule.runtime.core.api.MuleContext;
import org.mule.runtime.core.api.context.notification.FlowStackElement;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.core.api.processor.Processor;
import org.mule.runtime.core.api.processor.ReactiveProcessor;
import org.mule.runtime.core.internal.context.notification.DefaultFlowCallStack;
import org.mule.runtime.core.internal.exception.MessagingException;
import org.mule.runtime.core.internal.message.InternalEvent;
import org.mule.runtime.core.privileged.event.PrivilegedEvent;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;

import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.inject.Inject;

/**
 * Next-operation message processor implementation.
 *
 * Such implementation handles a set of callbacks to execute as next operations that are must be configured before processing the
 * event.
 *
 * @since 4.0
 */
public class PolicyNextActionMessageProcessor extends AbstractComponent implements Processor, Initialisable {

  private static final Logger LOGGER = getLogger(PolicyNextActionMessageProcessor.class);

  public static final String POLICY_NEXT_OPERATION = "policy.nextOperation";
  public static final String POLICY_STATE_EVENT = "policy.beforeNextEvent";

  @Inject
  private MuleContext muleContext;

  private PolicyNotificationHelper notificationHelper;

  private final PolicyEventConverter policyEventConverter = new PolicyEventConverter();

  @Override
  public CoreEvent process(CoreEvent event) throws MuleException {
    return processToApply(event, this);
  }

  private Consumer<CoreEvent> pushAfterNextFlowStackElement() {
    return event -> ((DefaultFlowCallStack) event.getFlowCallStack())
        .push(new FlowStackElement(toPolicyLocation(getLocation()), null));
  }

  private String toPolicyLocation(ComponentLocation componentLocation) {
    return componentLocation.getParts().get(0).getPartPath() + "/" + componentLocation.getParts().get(1).getPartPath()
        + "[after next]";
  }

  private Consumer<CoreEvent> popBeforeNextFlowFlowStackElement() {
    return event -> ((DefaultFlowCallStack) event.getFlowCallStack()).pop();
  }

  @Override
  public Publisher<CoreEvent> apply(Publisher<CoreEvent> publisher) {
    return from(publisher)
        .doOnNext(coreEvent -> logExecuteNextEvent("Before execute-next", coreEvent.getContext(),
                                                   coreEvent.getMessage(), muleContext.getConfiguration().getId()))
        .map(event -> (CoreEvent) policyEventConverter.createEvent(saveState((PrivilegedEvent) event),
                                                                   getOriginalEvent(event)))
        .doOnNext(event -> {
          popBeforeNextFlowFlowStackElement().accept(event);
          notificationHelper.notification(BEFORE_NEXT).accept(event);
        })
        .zipWith(subscriberContext().map(ctx -> ctx.get(POLICY_NEXT_OPERATION)))
        .flatMap(event -> just(event.getT1())
            .transform((ReactiveProcessor) event.getT2())
            .onErrorMap(MessagingException.class, t -> {
              notificationHelper.fireNotification(t.getEvent(), t, AFTER_NEXT);
              pushAfterNextFlowStackElement().accept(t.getEvent());

              for (Entry<String, ?> entry : ((InternalEvent) t.getEvent()).getInternalParameters().entrySet()) {
                if (POLICY_STATE_EVENT.equals(entry.getKey())) {
                  t.setProcessedEvent(policyEventConverter.createEvent((PrivilegedEvent) t.getEvent(),
                                                                       (PrivilegedEvent) entry.getValue()));
                  break;
                }
              }
              return t;
            }))
        .doOnNext(coreEvent -> {
          notificationHelper.fireNotification(coreEvent, null, AFTER_NEXT);
          pushAfterNextFlowStackElement().accept(coreEvent);
          logExecuteNextEvent("After execute-next", coreEvent.getContext(), coreEvent.getMessage(),
                              this.muleContext.getConfiguration().getId());
        })
        .map(result -> (CoreEvent) policyEventConverter.createEvent((PrivilegedEvent) result,
                                                                    loadState((PrivilegedEvent) result)));
  }

  private PrivilegedEvent getOriginalEvent(CoreEvent event) {
    final PrivilegedEvent operationOriginalEvent =
        ((InternalEvent) event).getInternalParameter(POLICY_OPERATION_ORIGINAL_EVENT);
    if (operationOriginalEvent != null) {
      return operationOriginalEvent;
    } else {
      return ((InternalEvent) event).getInternalParameter(POLICY_SOURCE_ORIGINAL_EVENT);
    }
  }

  private PrivilegedEvent saveState(PrivilegedEvent event) {
    return quickCopy(event, singletonMap(POLICY_STATE_EVENT, event));
  }

  private PrivilegedEvent loadState(PrivilegedEvent event) {
    return ((InternalEvent) event).getInternalParameter(POLICY_STATE_EVENT);
  }

  @Override
  public void initialise() throws InitialisationException {
    notificationHelper =
        new PolicyNotificationHelper(muleContext.getNotificationManager(), muleContext.getConfiguration().getId(), this);
  }

  private void logExecuteNextEvent(String startingMessage, EventContext eventContext, Message message, String policyName) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("\nEvent Id: " + eventContext.getCorrelationId() + "\n" + startingMessage + ".\nPolicy: " + policyName
          + "\n" + message.getAttributes().getValue().toString());
    }
  }

}
