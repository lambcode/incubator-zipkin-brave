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
package brave.messaging;

import brave.Span;
import brave.internal.Nullable;

/**
 * @param <Chan> the type of the channel
 * @param <Msg> the type of the message
 * @param <C> the type that carriers the trace context, usually headers
 */
// abstract class instead of interface to allow method adds before Java 1.8
public abstract class MessagingAdapter<Chan, Msg, C> {
  // TODO: make some of these methods not abstract as they don't have meaning for all impls

  /** Returns the trace context carrier from the message. Usually, this is headers. */
  public abstract C carrier(Msg message);

  /**
   * Messaging channel, e.g. kafka queue or JMS topic name. {@code null} if unreadable.
   *
   * <p>Conventionally associated with the key "message.channel"
   */
  @Nullable public abstract String channel(Chan channel);

  /**
   * Type of channel, e.g. queue or topic. {@code null} if unreadable.
   *
   * <p>Conventionally associated with the key "message.channel_type"
   */
  // TODO: naming is arbitrary.. we once used "kind" for Span but only because stackdriver did..
  // Not sure we should re-use kind for parity with that or not..
  @Nullable public abstract String channelType(Chan channel);

  /**
   * Key used to identity or partition messages. {@code null} if unreadable.
   *
   * <p>Conventionally associated with the key "message.key"
   */
  @Nullable public abstract String messageKey(Msg message);

  /**
   * Identifier used to correlate logs. {@code null} if unreadable.
   *
   * <p>Conventionally associated with the key "message.correlation_id"
   */
  @Nullable public abstract String correlationId(Msg message);

  /**
   * Message broker name. {@code null} if unreadable.
   *
   * <p>Conventionally associated with {@link Span#remoteServiceName(String)}
   */
  @Nullable public abstract String brokerName(Chan channel);

  protected MessagingAdapter() {
  }
}
