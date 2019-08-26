/*
 * Copyright 2013-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.features.handler;

import brave.ScopedSpan;
import brave.Tracing;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.handler.MutableSpan.AnnotationUpdater;
import brave.handler.MutableSpan.TagUpdater;
import brave.propagation.B3Propagation;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Test;
import zipkin2.Annotation;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class ExtraFieldFinishedSpanHandlerTest {
  private static final String KEY = "myKey";
  private static final FinishedSpanHandler finishedSpanHandler = new FinishedSpanHandler() {
    @Override public boolean handle(TraceContext context, MutableSpan span) {
      span.localServiceName(ExtraFieldPropagation.get(context, KEY));
      return true;
    }
  };
  private final List<Span> spans = new ArrayList<>();
  private final Tracing tracing = Tracing.newBuilder()
    .propagationFactory(ExtraFieldPropagation.newFactory(B3Propagation.FACTORY, KEY))
    .addFinishedSpanHandler(finishedSpanHandler)
    .addOrphanedSpanHandler(finishedSpanHandler)
    .spanReporter(spans::add)
    .build();

  @After public void close() {
    tracing.close();
  }

  @Test public void showTagFromExtra() {
    ScopedSpan span = tracing.tracer().startScopedSpan("span");
    try {
      ExtraFieldPropagation.set(KEY, "myservice");
    } finally {
      span.finish();
    }

    assertThat(spans.get(0).localServiceName()).isEqualTo("myservice");
  }

  @Test public void showTagFromExtraInOrphanedSpan() throws InterruptedException {
    createAndLeakSpan(); // done in separate method so we can force gc
    blockOnGC();
    tracing.tracer().nextSpan().abandon(); //create another span to trigger PendingSpans.reportOrphanedSpans()

    assertThat(spans.get(0).localServiceName()).isEqualTo("myservice");
  }

  private void createAndLeakSpan() {
    brave.Span span = tracing.tracer().nextSpan();
    span.annotate("something"); //span must not be empty for it to be reported when abandoned
    ExtraFieldPropagation.set(span.context(), KEY, "myservice");
  }

  private static void blockOnGC() throws InterruptedException {
    System.gc();
    Thread.sleep(200L);
  }
}
