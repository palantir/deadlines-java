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

package com.palantir.callcontext;

import static com.palantir.deadlines.DeadlinesHttpHeaders.EXPECT_WITHIN;
import static com.palantir.deadlines.DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import com.codahale.metrics.Meter;
import com.palantir.callcontext.DeadlineMetrics.Expired_Budget;
import com.palantir.callcontext.DeadlineMetrics.Expired_Cause;
import com.palantir.callcontext.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.DeadlineExpiredException;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Deadlines stored in the {@link CallContext}, and how they interact with the deadline that deadlines-java stores
 * for the trace. The mechanics of scopes and hand-offs are specified and tested by the call-context library; these
 * tests cover what deadlines add on top.
 */
// Shouldn't take this long. We should still bound the test execution time because some tests use threads that can block
@Timeout(10)
class CallContextDeadlinesTest {

    private static final Duration PROPOSED_DEADLINE = Duration.ofMinutes(5);
    private static final CallContext.Key<String> OTHER = CallContext.Key.create("other");

    // deadlines-java's clock can't be set, so a deadline stored for the trace elapses in real time.
    private static final Consumer<Duration> FIVE_SECOND_TRACE_DEADLINE =
            remaining -> assertThat(remaining).isBetween(Duration.ofSeconds(4), Duration.ofSeconds(5));

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
        assertThat(CallContextScope.currentScope())
                .as("the test left a scope open")
                .isNull();
    }

    @Nested
    class WhichDeadlineApplies {

        @Test
        void no_deadline_when_the_context_and_the_trace_have_none() {
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            assertThat(Deadlines.getEnforcement()).isEmpty();
        }

        @Test
        void a_deadline_in_the_context_overrides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (CallContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                assertThat(Deadlines.getRemainingDeadline()).hasValueSatisfying(FIVE_SECOND_TRACE_DEADLINE);
                try (CallContextScope ignored1 =
                        external(Duration.ofSeconds(9), Enforcement.DEFER).attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(9));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
                assertThat(Deadlines.getRemainingDeadline()).hasValueSatisfying(FIVE_SECOND_TRACE_DEADLINE);
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);
            }
        }

        @Test
        void a_context_without_a_deadline_does_not_hide_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (CallContextScope ignored1 =
                        CallContext.empty().with(OTHER, "value").attach()) {
                    assertThat(Deadlines.getRemainingDeadline())
                            .as("binding a context for another value does not change deadlines")
                            .hasValueSatisfying(FIVE_SECOND_TRACE_DEADLINE);
                }
            }
        }
    }

    @Nested
    class WithDeadline {

        @Test
        void binds_an_internal_deadline_with_deferred_enforcement_when_there_is_none() {
            try (CallContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(5))) {
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

        @ParameterizedTest
        @ValueSource(longs = {2, 5})
        void keeps_a_current_deadline_that_expires_sooner_or_at_the_same_time(long timeoutSeconds) {
            try (CallContextScope ignored =
                            external(Duration.ofSeconds(2), Enforcement.ENFORCE).attach();
                    CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(timeoutSeconds))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(2));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);

                clock.advance(Duration.ofSeconds(2));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .as("the external deadline is unchanged")
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }

        @Test
        void shortens_a_current_deadline_and_keeps_its_enforcement() {
            try (CallContextScope ignored = external(Duration.ofSeconds(10), Enforcement.ENFORCE)
                            .attach();
                    CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
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

                try (CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);
                }
            }
        }

        @Test
        void extends_the_deadline_only_inside_without_deadline() {
            try (CallContextScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    CallContextScope ignored1 = Deadlines.withoutDeadline();
                    CallContextScope ignored2 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
            }
        }

        @ParameterizedTest
        @ValueSource(longs = {0, -1000})
        void binds_an_expired_deadline_for_a_zero_or_negative_timeout(long timeoutMillis) {
            try (CallContextScope ignored = Deadlines.withDeadline(Duration.ofMillis(timeoutMillis))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ZERO);
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void saturates_timeouts_too_long_to_represent_in_nanoseconds() {
            try (CallContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(Long.MAX_VALUE))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofNanos(Long.MAX_VALUE));
            }
        }

        @Test
        void keeps_the_other_values_of_the_context() {
            try (CallContextScope ignored =
                            CallContext.empty().with(OTHER, "value").attach();
                    CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(CallContext.current().get(OTHER)).isEqualTo("value");
            }
        }

        @Test
        void opening_and_closing_scopes_never_checks_the_deadline() {
            try (CallContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));
                CallContext expired = CallContext.current();

                assertThatCode(() -> {
                            try (CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1));
                                    CallContextScope ignored2 = Deadlines.withoutDeadline();
                                    CallContextScope ignored3 = expired.attach()) {
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
            try (CallContextScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));

                try (CallContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    assertThat(Deadlines.getEnforcement()).isEmpty();
                    assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                            .doesNotThrowAnyException();
                }
            }
        }

        @Test
        void requests_carry_only_the_proposed_deadline_and_client_enforcement() {
            try (CallContextScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    CallContextScope ignored1 = Deadlines.withoutDeadline()) {
                assertThat(encode(Enforcement.DEFER)).containsOnly(entry(EXPECT_WITHIN, "300.000"));
                assertThat(encode(Enforcement.ENFORCE))
                        .containsOnly(entry(EXPECT_WITHIN, "300.000"), entry(EXPECT_WITHIN_ENFORCED, "true"));
            }
        }

        @Test
        void drops_the_enforcement_of_the_current_deadline() {
            CallContext enforcementDisabled = Deadlines.withRequestDeadline(
                    CallContext.empty(),
                    Optional.empty(),
                    Map.of(EXPECT_WITHIN, "1", EXPECT_WITHIN_ENFORCED, "false"),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
            try (CallContextScope ignored = enforcementDisabled.attach()) {
                assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "false");

                try (CallContextScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "true");
                }
            }
        }

        @Test
        void keeps_the_other_values_of_the_context() {
            try (CallContextScope ignored =
                            CallContext.empty().with(OTHER, "value").attach();
                    CallContextScope ignored1 = Deadlines.withoutDeadline()) {
                assertThat(CallContext.current().get(OTHER)).isEqualTo("value");
            }
        }
    }

    @Nested
    class RequestDeadlines {

        @Test
        void adds_the_deadline_to_the_given_context_without_binding_it() {
            CallContext base = CallContext.empty().with(OTHER, "value");

            CallContext parsed = Deadlines.withRequestDeadline(
                    base, Optional.empty(), Map.of(EXPECT_WITHIN, "2"), MapAdapter.INSTANCE, Enforcement.ENFORCE);

            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            assertThat(parsed.get(OTHER)).isEqualTo("value");
            try (CallContextScope ignored = parsed.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(2));
            }
        }

        @Test
        void uses_the_shorter_of_the_header_and_internal_deadlines_preferring_the_header_on_a_tie() {
            CallContext internalShorter = parse(Duration.ofSeconds(2), Optional.of(Duration.ofSeconds(1)));
            CallContext headerShorter = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(2)));
            CallContext tie = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(1)));
            clock.advance(Duration.ofSeconds(1));

            assertExpired(internalShorter, DeadlineExpiredException.Internal.class);
            assertExpired(headerShorter, DeadlineExpiredException.External.class);
            assertExpired(tie, DeadlineExpiredException.External.class);
        }

        @Test
        void a_request_without_a_deadline_hides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);
                CallContext parsed = Deadlines.withRequestDeadline(
                        CallContext.empty(), Optional.empty(), Map.of(), MapAdapter.INSTANCE, Enforcement.ENFORCE);

                try (CallContextScope ignored1 = parsed.attach()) {
                    assertThat(Deadlines.getRemainingDeadline())
                            .as("a request without a deadline does not inherit the trace state")
                            .isEmpty();
                }
            }
        }

        private CallContext parse(Duration header, Optional<Duration> internalDeadline) {
            return Deadlines.withRequestDeadline(
                    CallContext.empty(),
                    internalDeadline,
                    Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(header.toNanos())),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
        }

        private void assertExpired(CallContext context, Class<? extends DeadlineExpiredException> expected) {
            try (CallContextScope ignored = context.attach()) {
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(expected);
            }
        }
    }

    @Nested
    class Disarming {

        @Test
        void a_disarmed_request_deadline_is_no_deadline() throws Exception {
            CallContext request = external(Duration.ofSeconds(1), Enforcement.ENFORCE);
            clock.advance(Duration.ofSeconds(2));

            Deadlines.disarmRequestDeadline(request);

            try (CallContextScope ignored = request.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                assertThat(Deadlines.getEnforcement()).isEmpty();
                assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .doesNotThrowAnyException();
                assertThat(Deadlines.awaitWithinDeadline(RecordingFuture.pending(), Enforcement.ENFORCE))
                        .isEqualTo("value");
                assertThat(encode(Enforcement.DEFER)).containsOnly(entry(EXPECT_WITHIN, "300.000"));
            }
        }

        @Test
        void background_work_started_by_the_request_no_longer_fails_once_disarmed() throws Exception {
            CallContext request = external(Duration.ofSeconds(1), Enforcement.ENFORCE);
            CountDownLatch committed = new CountDownLatch(1);
            Future<Optional<Duration>> background;
            try (CallContextScope ignored = request.attach()) {
                background = CallContext.propagating(executor).submit(() -> {
                    committed.await();
                    Deadlines.checkDeadline(Enforcement.ENFORCE);
                    return Deadlines.getRemainingDeadline();
                });
            }
            clock.advance(Duration.ofSeconds(2));

            Deadlines.disarmRequestDeadline(request);
            committed.countDown();

            assertThat(background.get()).isEmpty();
        }

        @Test
        void a_wait_in_progress_when_the_request_is_disarmed_continues_without_a_timeout() throws Exception {
            CallContext request = external(Duration.ofSeconds(1), Enforcement.ENFORCE);
            RecordingFuture future = RecordingFuture.timingOut(() -> Deadlines.disarmRequestDeadline(request));
            long expiredBefore = expiredMeter(Expired_Cause.EXTERNAL).getCount();
            try (CallContextScope ignored = request.attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get(1000000000ns)", "get()");
            assertThat(expiredMeter(Expired_Cause.EXTERNAL).getCount())
                    .as("a disarmed deadline does not expire")
                    .isEqualTo(expiredBefore);
        }

        @Test
        void deadlines_shortened_from_a_request_deadline_are_disarmed_with_it() {
            CallContext request = external(Duration.ofSeconds(10), Enforcement.ENFORCE);
            CallContext shortened;
            try (CallContextScope ignored = request.attach();
                    CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                shortened = CallContext.current();
            }

            Deadlines.disarmRequestDeadline(request);

            try (CallContextScope ignored = shortened.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            }
        }

        @Test
        void a_deadline_bound_after_disarming_applies() {
            CallContext request = external(Duration.ofSeconds(10), Enforcement.ENFORCE);

            Deadlines.disarmRequestDeadline(request);

            try (CallContextScope ignored = request.attach();
                    CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                assertThat(Deadlines.getEnforcement())
                        .as("as if there were no current deadline")
                        .contains(Enforcement.DEFER);
            }
        }

        @Test
        void disarming_one_request_leaves_other_requests_armed() {
            CallContext request = external(Duration.ofSeconds(10), Enforcement.ENFORCE);
            CallContext otherRequest = external(Duration.ofSeconds(10), Enforcement.ENFORCE);

            Deadlines.disarmRequestDeadline(request);

            try (CallContextScope ignored = otherRequest.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
            }
        }

        @Test
        void disarming_does_nothing_to_a_deadline_not_parsed_from_a_request() {
            try (CallContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                Deadlines.disarmRequestDeadline(CallContext.current());

                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
            }
            assertThatCode(() -> Deadlines.disarmRequestDeadline(CallContext.empty()))
                    .doesNotThrowAnyException();
        }

        @Test
        void a_disarmed_request_deadline_hides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);
                CallContext request = external(Duration.ofSeconds(10), Enforcement.ENFORCE);

                Deadlines.disarmRequestDeadline(request);

                try (CallContextScope ignored1 = request.attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
            }
        }
    }

    @Nested
    class HandOffs {

        @Test
        void a_deadline_is_handed_off_with_the_call_context() throws Exception {
            ExecutorService propagating = CallContext.propagating(executor);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> blocker = executor.submit(() -> {
                release.await();
                return null;
            });

            Future<Optional<Duration>> task;
            try (CallContextScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
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

                try (CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    assertThat(executor.submit(Tracers.wrap("test", Deadlines::getRemainingDeadline))
                                    .get())
                            .hasValueSatisfying(FIVE_SECOND_TRACE_DEADLINE);
                }
            }
        }
    }

    @Nested
    class AwaitingSharedWork {

        @Test
        void throws_when_the_deadline_expires_while_waiting_without_cancelling_the_future() {
            RecordingFuture future = RecordingFuture.timingOut();
            long expiredBefore = expiredMeter(Expired_Cause.EXTERNAL).getCount();
            try (CallContextScope ignored =
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
            try (CallContextScope ignored =
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
            try (CallContextScope ignored =
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
            try (CallContextScope ignored =
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
            ExecutorService requestScopedExecutor = CallContext.propagating(otherExecutor);

            // Caller 1 has 50ms left and misses, so it starts the load. The cache's executor here propagates the
            // call context, to show that the load's own scope is what removes the caller's deadline.
            Future<String> load;
            try (CallContextScope ignored =
                    external(Duration.ofMillis(50), Enforcement.ENFORCE).attach()) {
                load = CallContext.propagating(executor).submit(() -> {
                    try (CallContextScope ignored1 = Deadlines.withoutDeadline()) {
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
                try (CallContextScope ignored =
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
    class TraceState {

        @Test
        void with_deadline_treats_disabled_trace_state_as_no_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(1), Enforcement.ENFORCE);
                com.palantir.deadlines.Deadlines.disableFurtherDeadlinePropagation();

                try (CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
            }
        }

        @Test
        void with_deadline_keeps_a_trace_deadline_that_expires_sooner() throws Exception {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (CallContextScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                    assertThat(Deadlines.getRemainingDeadline()).hasValueSatisfying(FIVE_SECOND_TRACE_DEADLINE);
                    assertThat(executor.submit(CallContext.current().wrap(Deadlines::getRemainingDeadline))
                                    .get())
                            .as("the context is unchanged, so it carries no deadline")
                            .isEmpty();
                }
            }
        }

        @Test
        void requests_carry_the_trace_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                Map<String, String> headers = encode(Enforcement.DEFER);
                assertThat(Deadlines.tryParseSecondsToNanoseconds(headers.get(EXPECT_WITHIN)))
                        .isBetween(
                                Duration.ofSeconds(4).toNanos(),
                                Duration.ofSeconds(5).toNanos());
                assertThat(headers).containsEntry(EXPECT_WITHIN_ENFORCED, "true");
            }
        }

        @Test
        void awaiting_ignores_the_trace_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ZERO, Enforcement.ENFORCE);

                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }
    }

    // A context with an external deadline, as parsed from an inbound request.
    private static CallContext external(Duration deadline, Enforcement enforcement) {
        return Deadlines.withRequestDeadline(
                CallContext.empty(),
                Optional.empty(),
                Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(deadline.toNanos())),
                MapAdapter.INSTANCE,
                enforcement);
    }

    // A context with an internal deadline, as configured for an endpoint.
    private static CallContext internal(Duration deadline, Enforcement enforcement) {
        return Deadlines.withRequestDeadline(
                CallContext.empty(), Optional.of(deadline), Map.of(), MapAdapter.INSTANCE, enforcement);
    }

    // Stores an internal deadline for the current trace, as deadlines-java does for a request.
    private static void setTraceDeadline(Duration deadline, Enforcement enforcement) {
        com.palantir.deadlines.Deadlines.parseFromRequest(
                Optional.of(deadline), Map.of(), MapAdapter.INSTANCE, enforcement);
    }

    private static Map<String, String> encode(Enforcement clientEnforcement) {
        return encode(PROPOSED_DEADLINE, clientEnforcement);
    }

    private static Map<String, String> encode(Duration proposedDeadline, Enforcement clientEnforcement) {
        Map<String, String> headers = new HashMap<>();
        Deadlines.encodeToRequest(proposedDeadline, headers, MapAdapter.INSTANCE, clientEnforcement);
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
        private final Runnable whileWaiting;
        private final List<String> calls = new ArrayList<>();

        private RecordingFuture(boolean done, boolean timesOut, boolean fails, Runnable whileWaiting) {
            this.done = done;
            this.timesOut = timesOut;
            this.fails = fails;
            this.whileWaiting = whileWaiting;
        }

        // Not complete yet, but completes within any timeout.
        static RecordingFuture pending() {
            return new RecordingFuture(false, false, false, () -> {});
        }

        static RecordingFuture timingOut() {
            return timingOut(() -> {});
        }

        // Times out, running whileWaiting before it does, as if it happened while the caller waited.
        static RecordingFuture timingOut(Runnable whileWaiting) {
            return new RecordingFuture(false, true, false, whileWaiting);
        }

        static RecordingFuture completed() {
            return new RecordingFuture(true, false, false, () -> {});
        }

        static RecordingFuture failing() {
            return new RecordingFuture(false, false, true, () -> {});
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
                whileWaiting.run();
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
}
