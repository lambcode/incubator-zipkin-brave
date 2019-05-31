/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package brave.jms;

import brave.Span;
import brave.internal.Nullable;
import brave.messaging.ProducerHandler;
import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.Message;

/**
 * Decorates, then finishes a producer span. Allows tracing to message the duration between batching
 * for send and actual send.
 */
@JMS2_0 class TracingCompletionListener<Msg> implements CompletionListener {
  static <Msg> TracingCompletionListener<Msg> create(@Nullable CompletionListener delegate,
    ProducerHandler<Destination, Msg, Msg> handler, CurrentTraceContext current,
    Destination destination, Msg message, Span span) {
    if (delegate == null) {
      return new TracingCompletionListener<>(handler, destination, message, span);
    }
    return new TracingForwardingCompletionListener<>(delegate, handler, current, destination,
      message, span);
  }

  final ProducerHandler<Destination, Msg, Msg> handler;
  final Destination destination;
  final Msg message;
  final Span span;

  TracingCompletionListener(ProducerHandler<Destination, Msg, Msg> handler, Destination destination,
    Msg message, Span span) {
    this.handler = handler;
    this.destination = destination;
    this.message = message;
    this.span = span;
  }

  @Override public void onCompletion(Message message) {
    finish(null);
  }

  @Override public void onException(Message message, Exception exception) {
    finish(exception);
  }

  void finish(@Nullable Throwable error) {
    if (error != null) span.error(error);
    handler.finishSend(destination, message, span);
  }
}

final class TracingForwardingCompletionListener<Msg> extends TracingCompletionListener<Msg> {
  final CompletionListener delegate;
  final CurrentTraceContext current;

  TracingForwardingCompletionListener(CompletionListener delegate,
    ProducerHandler<Destination, Msg, Msg> handler, CurrentTraceContext current,
    Destination destination, Msg message, Span span) {
    super(handler, destination, message, span);
    this.delegate = delegate;
    this.current = current;
  }

  @Override public void onCompletion(Message message) {
    try (Scope ws = current.maybeScope(span.context())) {
      delegate.onCompletion(message);
    } finally {
      finish(null);
    }
  }

  @Override public void onException(Message message, Exception exception) {
    try (Scope ws = current.maybeScope(span.context())) {
      delegate.onException(message, exception);
    } finally {
      finish(exception);
    }
  }
}
