/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.deadlines;

import static com.palantir.deadlines.DeadlinesHttpHeaders.EXPECT_WITHIN;
import static com.palantir.deadlines.DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.codahale.metrics.Meter;
import com.palantir.deadlines.DeadlineMetrics.Expired_Budget;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.requestcontext.RequestContext;
import com.palantir.requestcontext.RequestContextScope;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Deadlines stored in the {@link RequestContext}, and how they interact with the deadline stored for the trace. The
 * mechanics of scopes and hand-offs are specified and tested by the request-context library; these tests cover what
 * deadlines add on top.
 */
@Timeout(10)
class RequestContextDeadlinesTest {

    private static final Duration PROPOSED_DEADLINE = Duration.ofMinutes(5);
    private static final RequestContext.Key<String> OTHER = RequestContext.Key.create("other");

    private final TestClock clock = new TestClock();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutorService otherExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService thirdExecutor = Executors.newSingleThreadExecutor();

    @BeforeEach
    void beforeEach() {
        Deadlines.setClock(clock);
    }

    @AfterEach
    void afterEach() {
        executor.shutdownNow();
        otherExecutor.shutdownNow();
        thirdExecutor.shutdownNow();
        Deadlines.setClock(System::nanoTime);
        assertThat(RequestContext.current()).as("the test left a scope open").isSameAs(RequestContext.empty());
    }

    @Nested
    class WhichDeadlineApplies {

        @Test
        void no_deadline_when_the_context_and_the_trace_have_none() {
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            assertThat(Deadlines.getEnforcement()).isEmpty();
        }

        @Test
        void trace_state_applies_when_the_context_has_no_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);
            }
        }

        @Test
        void a_deadline_in_the_context_overrides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                try (RequestContextScope ignored1 =
                        external(Duration.ofSeconds(9), Enforcement.DEFER).attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(9));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
            }
        }

        @Test
        void a_context_without_a_deadline_does_not_hide_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (RequestContextScope ignored1 =
                        RequestContext.empty().with(OTHER, "value").attach()) {
                    assertThat(Deadlines.getRemainingDeadline())
                            .as("binding a context for another value does not change deadlines")
                            .contains(Duration.ofSeconds(5));
                }
            }
        }

        @Test
        void trace_state_applies_again_once_the_scope_closes() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
            }
        }
    }

    @Nested
    class WithDeadline {

        @Test
        void binds_an_internal_deadline_with_deferred_enforcement_when_there_is_none() {
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);

                clock.advance(Duration.ofSeconds(6));

                assertThatCode(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .as("enforcement is deferred to the caller")
                        .doesNotThrowAnyException();
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void keeps_a_current_deadline_that_expires_sooner() {
            try (RequestContextScope ignored =
                            external(Duration.ofSeconds(2), Enforcement.ENFORCE).attach();
                    RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(2));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);

                clock.advance(Duration.ofSeconds(2));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .as("the external deadline is unchanged")
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }

        @Test
        void keeps_the_current_deadline_on_a_tie() {
            try (RequestContextScope ignored =
                            external(Duration.ofSeconds(2), Enforcement.ENFORCE).attach();
                    RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(2))) {
                clock.advance(Duration.ofSeconds(2));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }

        @Test
        void shortens_a_current_deadline_and_keeps_its_enforcement() {
            try (RequestContextScope ignored = external(Duration.ofSeconds(10), Enforcement.ENFORCE)
                            .attach();
                    RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);

                clock.advance(Duration.ofSeconds(1));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void shortens_a_deadline_stored_for_the_trace() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(10), Enforcement.ENFORCE);

                try (RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);
                }
            }
        }

        @Test
        void never_extends_the_current_deadline() {
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1));
                    RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
            }
        }

        @Test
        void extends_the_deadline_only_inside_without_deadline() {
            try (RequestContextScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    RequestContextScope ignored1 = Deadlines.withoutDeadline();
                    RequestContextScope ignored2 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
            }
        }

        @ParameterizedTest
        @ValueSource(longs = {0, -1000})
        void binds_an_expired_deadline_for_a_zero_or_negative_timeout(long timeoutMillis) {
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofMillis(timeoutMillis))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ZERO);
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void saturates_timeouts_too_long_to_represent_in_nanoseconds() {
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(Long.MAX_VALUE))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofNanos(Long.MAX_VALUE));
            }
        }

        @Test
        void keeps_the_other_values_of_the_context() {
            try (RequestContextScope ignored =
                            RequestContext.empty().with(OTHER, "value").attach();
                    RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(RequestContext.current().get(OTHER)).isEqualTo("value");
            }
        }

        @Test
        void opening_and_closing_scopes_never_checks_the_deadline() {
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));
                RequestContext expired = RequestContext.current();

                assertThatCode(() -> {
                            try (RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1));
                                    RequestContextScope ignored2 = Deadlines.withoutDeadline();
                                    RequestContextScope ignored3 = expired.attach()) {
                                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ZERO);
                            }
                        })
                        .doesNotThrowAnyException();
            }
        }
    }

    @Nested
    class WithoutDeadline {

        @Test
        void hides_the_current_deadline() {
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));

                try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    assertThat(Deadlines.getEnforcement()).isEmpty();
                    assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                            .doesNotThrowAnyException();
                }
            }
        }

        @Test
        void requests_carry_only_the_proposed_deadline_and_client_enforcement() {
            try (RequestContextScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                assertThat(encode(Enforcement.DEFER)).containsOnly(entry(EXPECT_WITHIN, "300.000"));
                assertThat(encode(Enforcement.ENFORCE))
                        .containsOnly(entry(EXPECT_WITHIN, "300.000"), entry(EXPECT_WITHIN_ENFORCED, "true"));
            }
        }

        @Test
        void drops_the_enforcement_of_the_current_deadline() {
            RequestContext enforcementDisabled = Deadlines.withRequestDeadline(
                    RequestContext.empty(),
                    Optional.empty(),
                    Map.of(EXPECT_WITHIN, "1", EXPECT_WITHIN_ENFORCED, "false"),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
            try (RequestContextScope ignored = enforcementDisabled.attach()) {
                assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "false");

                try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "true");
                }
            }
        }

        @Test
        void keeps_the_other_values_of_the_context() {
            try (RequestContextScope ignored =
                            RequestContext.empty().with(OTHER, "value").attach();
                    RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                assertThat(RequestContext.current().get(OTHER)).isEqualTo("value");
            }
        }
    }

    @Nested
    class RequestDeadlines {

        @Test
        void adds_the_deadline_to_the_given_context_without_binding_it() {
            RequestContext base = RequestContext.empty().with(OTHER, "value");

            RequestContext parsed = Deadlines.withRequestDeadline(
                    base, Optional.empty(), Map.of(EXPECT_WITHIN, "2"), MapAdapter.INSTANCE, Enforcement.ENFORCE);

            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            assertThat(parsed.get(OTHER)).isEqualTo("value");
            try (RequestContextScope ignored = parsed.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(2));
            }
        }

        @Test
        void uses_the_shorter_of_the_header_and_internal_deadlines_preferring_the_header_on_a_tie() {
            RequestContext internalShorter = parse(Duration.ofSeconds(2), Optional.of(Duration.ofSeconds(1)));
            RequestContext headerShorter = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(2)));
            RequestContext tie = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(1)));
            clock.advance(Duration.ofSeconds(1));

            assertExpired(internalShorter, DeadlineExpiredException.Internal.class);
            assertExpired(headerShorter, DeadlineExpiredException.External.class);
            assertExpired(tie, DeadlineExpiredException.External.class);
        }

        @Test
        void a_request_without_a_deadline_hides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);
                RequestContext parsed = Deadlines.withRequestDeadline(
                        RequestContext.empty(), Optional.empty(), Map.of(), MapAdapter.INSTANCE, Enforcement.ENFORCE);

                try (RequestContextScope ignored1 = parsed.attach()) {
                    assertThat(Deadlines.getRemainingDeadline())
                            .as("a request without a deadline does not inherit the trace state")
                            .isEmpty();
                }
            }
        }

        private RequestContext parse(Duration header, Optional<Duration> internalDeadline) {
            return Deadlines.withRequestDeadline(
                    RequestContext.empty(),
                    internalDeadline,
                    Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(header.toNanos())),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
        }

        private void assertExpired(RequestContext context, Class<? extends DeadlineExpiredException> expected) {
            try (RequestContextScope ignored = context.attach()) {
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(expected);
            }
        }
    }

    @Nested
    class HandOffs {

        @Test
        void a_deadline_is_handed_off_with_the_request_context() throws Exception {
            ExecutorService propagating = RequestContext.propagating(executor);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> blocker = executor.submit(() -> {
                release.await();
                return null;
            });

            Future<Optional<Duration>> task;
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                task = propagating.submit(Deadlines::getRemainingDeadline);
            }
            clock.advance(Duration.ofMillis(300));
            release.countDown();
            blocker.get();

            assertThat(task.get())
                    .as("runs after the scope closed, and the deadline kept elapsing")
                    .contains(Duration.ofMillis(700));
        }

        @Test
        void tracing_hand_offs_carry_only_trace_state() throws Exception {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.DEFER);

                try (RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    assertThat(executor.submit(Tracers.wrap("test", Deadlines::getRemainingDeadline))
                                    .get())
                            .contains(Duration.ofSeconds(5));
                }
            }
        }

        @Test
        void capturing_the_request_context_does_not_capture_trace_state() throws Exception {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.DEFER);

                assertThat(executor.submit(RequestContext.current().wrap(Deadlines::getRemainingDeadline))
                                .get())
                        .as("the deadline is stored for the trace, which only tracing carries")
                        .isEmpty();
            }
        }

        @Test
        void removing_the_deadline_on_one_thread_never_affects_another_thread() throws Exception {
            ExecutorService propagating = RequestContext.propagating(executor);
            ExecutorService otherPropagating = RequestContext.propagating(otherExecutor);
            CountDownLatch loadDetached = new CountDownLatch(1);
            CountDownLatch backgroundChecked = new CountDownLatch(1);

            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                Future<Optional<Duration>> backgroundSees = propagating.submit(() -> {
                    loadDetached.await();
                    return Deadlines.getRemainingDeadline();
                });
                Future<Optional<Duration>> loadSees = otherPropagating.submit(() -> {
                    try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                        loadDetached.countDown();
                        backgroundChecked.await();
                        return Deadlines.getRemainingDeadline();
                    }
                });

                assertThat(backgroundSees.get())
                        .as("background work keeps the request's deadline while the load has none")
                        .contains(Duration.ofSeconds(1));
                backgroundChecked.countDown();
                assertThat(loadSees.get()).isEmpty();
                assertThat(Deadlines.getRemainingDeadline())
                        .as("the request thread keeps its deadline")
                        .contains(Duration.ofSeconds(1));
            }
        }
    }

    @Nested
    class AwaitingSharedWork {

        @Test
        void waits_without_a_timeout_when_there_is_no_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();

            assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.ENFORCE))
                    .isEqualTo("value");
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void waits_without_a_timeout_when_the_deadline_is_not_enforced() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DEFER).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void disabled_enforcement_is_never_enforced() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DISABLE).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.ENFORCE))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void client_enforcement_enforces_a_deferred_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DEFER).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.ENFORCE))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get(1000000000ns)");
        }

        @Test
        void waits_at_most_the_remaining_time_of_an_enforced_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofMillis(300));

                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get(700000000ns)");
        }

        @Test
        void throws_when_the_deadline_expires_while_waiting_without_cancelling_the_future() {
            RecordingFuture future = RecordingFuture.timingOut();
            long expiredBefore = expiredMeter(Expired_Cause.EXTERNAL).getCount();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                assertThatThrownBy(() -> Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
            assertThat(future.calls).containsExactly("get(1000000000ns)");
            assertThat(expiredMeter(Expired_Cause.EXTERNAL).getCount()).isEqualTo(expiredBefore + 1);
        }

        @Test
        void throws_without_waiting_if_the_deadline_has_already_expired() {
            RecordingFuture future = RecordingFuture.pending();
            try (RequestContextScope ignored =
                    internal(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));

                assertThatThrownBy(() -> Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
            assertThat(future.calls).isEmpty();
        }

        @Test
        void returns_the_result_of_a_completed_future_even_after_the_deadline_expired() throws Exception {
            RecordingFuture future = RecordingFuture.completed();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));

                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void propagates_the_failure_of_the_future() {
            RecordingFuture future = RecordingFuture.failing();
            try (RequestContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                assertThatThrownBy(() -> Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isInstanceOf(ExecutionException.class);
            }
        }

        @Test
        void a_shared_load_is_bounded_by_no_callers_deadline_and_each_caller_waits_until_its_own() throws Exception {
            CountDownLatch releaseLoad = new CountDownLatch(1);
            AtomicReference<Map<String, String>> loadRequest = new AtomicReference<>();
            AtomicReference<Optional<Duration>> loadSubtaskSees = new AtomicReference<>();
            ExecutorService requestScopedExecutor = RequestContext.propagating(otherExecutor);

            // Caller 1 has 50ms left and misses, so it starts the load. The cache's executor here propagates the
            // request context, to show that the load's own scope is what removes the caller's deadline.
            Future<String> load;
            try (RequestContextScope ignored =
                    external(Duration.ofMillis(50), Enforcement.ENFORCE).attach()) {
                load = RequestContext.propagating(executor).submit(() -> {
                    try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                        loadRequest.set(encode(Enforcement.DEFER));
                        loadSubtaskSees.set(requestScopedExecutor
                                .submit(Deadlines::getRemainingDeadline)
                                .get());
                        releaseLoad.await();
                        return "value";
                    }
                });

                assertThatThrownBy(() -> Deadlines.awaitWithinDeadline(load, Enforcement.DEFER))
                        .as("caller 1 stops waiting at its own deadline")
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }

            // Caller 2 has 10s left and waits on the same load.
            Future<String> caller2 = thirdExecutor.submit(() -> {
                try (RequestContextScope ignored =
                        external(Duration.ofSeconds(10), Enforcement.ENFORCE).attach()) {
                    return Deadlines.awaitWithinDeadline(load, Enforcement.DEFER);
                }
            });
            releaseLoad.countDown();

            assertThat(caller2.get()).isEqualTo("value");
            assertThat(load.isCancelled())
                    .as("caller 1 giving up does not cancel the load")
                    .isFalse();
            assertThat(loadRequest.get())
                    .as("the load's requests carry only the client's proposed deadline")
                    .containsOnly(entry(EXPECT_WITHIN, "300.000"));
            assertThat(loadSubtaskSees.get())
                    .as("work the load hands off has no deadline either")
                    .isEmpty();
        }
    }

    @Nested
    @SuppressWarnings("deprecation") // exercises deprecated methods
    class DeprecatedMethods {

        @Test
        void disabling_propagation_does_not_affect_a_deadline_in_the_context() {
            try (RequestContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                Deadlines.disableFurtherDeadlinePropagation();

                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                assertThat(encode(Enforcement.DEFER)).containsOnly(entry(EXPECT_WITHIN, "1.000"));
            }
        }

        @Test
        void disabling_propagation_disables_trace_state_where_the_context_has_no_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (RequestContextScope ignored1 =
                        RequestContext.empty().with(OTHER, "value").attach()) {
                    Deadlines.disableFurtherDeadlinePropagation();

                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    assertThat(encode(Enforcement.ENFORCE))
                            .as("no deadline headers, as before")
                            .isEmpty();
                }
            }
        }

        @Test
        void parsing_into_trace_state_does_not_apply_where_the_context_has_a_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                try (RequestContextScope ignored1 = Deadlines.withoutDeadline()) {
                    setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
            }
        }

        @Test
        void with_deadline_treats_disabled_trace_state_as_no_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(1), Enforcement.ENFORCE);
                Deadlines.disableFurtherDeadlinePropagation();

                try (RequestContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
            }
        }
    }

    // A context with an external deadline, as parsed from an inbound request.
    private static RequestContext external(Duration deadline, Enforcement enforcement) {
        return Deadlines.withRequestDeadline(
                RequestContext.empty(),
                Optional.empty(),
                Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(deadline.toNanos())),
                MapAdapter.INSTANCE,
                enforcement);
    }

    // A context with an internal deadline, as configured for an endpoint.
    private static RequestContext internal(Duration deadline, Enforcement enforcement) {
        return Deadlines.withRequestDeadline(
                RequestContext.empty(), Optional.of(deadline), Map.of(), MapAdapter.INSTANCE, enforcement);
    }

    @SuppressWarnings("deprecation") // the trace state can only be set by the deprecated method
    private static void setTraceDeadline(Duration deadline, Enforcement enforcement) {
        Deadlines.parseFromRequest(Optional.of(deadline), Map.of(), MapAdapter.INSTANCE, enforcement);
    }

    private static Map<String, String> encode(Enforcement clientEnforcement) {
        Map<String, String> headers = new HashMap<>();
        Deadlines.encodeToRequest(PROPOSED_DEADLINE, headers, MapAdapter.INSTANCE, clientEnforcement);
        return headers;
    }

    @SuppressWarnings("for-rollout:deprecation")
    private static Meter expiredMeter(Expired_Cause cause) {
        return DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton())
                .expired()
                .cause(cause)
                .intent(Expired_Intent.THROW)
                .budget(Expired_Budget.SUB_10S)
                .build();
    }

    private enum MapAdapter
            implements RequestEncodingAdapter<Map<String, String>>, RequestDecodingAdapter<Map<String, String>> {
        INSTANCE;

        @Override
        public void setHeader(Map<String, String> headers, String headerName, String headerValue) {
            headers.put(headerName, headerValue);
        }

        @Override
        public Optional<String> getFirstHeader(Map<String, String> _headers, String _headerName) {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public @Nullable String maybeFirstHeader(Map<String, String> headers, String headerName) {
            return headers.get(headerName);
        }
    }

    /** Records how it is waited on, so tests can assert exactly how long a caller waits. */
    private static final class RecordingFuture implements Future<String> {
        private final boolean done;
        private final boolean timesOut;
        private final boolean fails;
        private final List<String> calls = new ArrayList<>();

        private RecordingFuture(boolean done, boolean timesOut, boolean fails) {
            this.done = done;
            this.timesOut = timesOut;
            this.fails = fails;
        }

        // Not complete yet, but completes within any timeout.
        static RecordingFuture pending() {
            return new RecordingFuture(false, false, false);
        }

        static RecordingFuture timingOut() {
            return new RecordingFuture(false, true, false);
        }

        static RecordingFuture completed() {
            return new RecordingFuture(true, false, false);
        }

        static RecordingFuture failing() {
            return new RecordingFuture(false, false, true);
        }

        @Override
        public boolean cancel(boolean _mayInterruptIfRunning) {
            calls.add("cancel");
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public String get() throws ExecutionException {
            calls.add("get()");
            return result();
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
            calls.add("get(" + unit.toNanos(timeout) + "ns)");
            if (timesOut) {
                throw new TimeoutException();
            }
            return result();
        }

        private String result() throws ExecutionException {
            if (fails) {
                throw new ExecutionException(new SafeRuntimeException("load failed"));
            }
            return "value";
        }
    }

    private static final class TestClock implements Deadlines.Clock {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime() {
            return now.get();
        }

        void advance(Duration duration) {
            now.addAndGet(duration.toNanos());
        }
    }
}
