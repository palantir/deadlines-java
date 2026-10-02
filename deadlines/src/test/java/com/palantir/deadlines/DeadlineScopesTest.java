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
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
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
 * Each test corresponds to a scenario in docs/deadline-contexts.md.
 */
@Timeout(10)
class DeadlineScopesTest {

    private static final Duration PROPOSED_DEADLINE = Duration.ofMinutes(5);

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
        assertThat(DeadlineScope.currentBinding())
                .as("the test left a scope open")
                .isNull();
    }

    @Nested
    class WhichDeadlineApplies {

        @Test
        void no_deadline_when_no_scope_is_open_and_the_trace_has_none() {
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            assertThat(Deadlines.getEnforcement()).isEmpty();
        }

        @Test
        void trace_state_applies_when_no_scope_is_open() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);
            }
        }

        @Test
        void an_open_scope_overrides_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                try (DeadlineScope ignored1 =
                        external(Duration.ofSeconds(9), Enforcement.DEFER).attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(9));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
            }
        }

        @Test
        void trace_state_applies_again_once_every_scope_is_closed() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                    assertEmptyInsideAndAfterNestedScope();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
            }
        }

        private void assertEmptyInsideAndAfterNestedScope() {
            try (DeadlineScope ignored = Deadlines.withoutDeadline()) {
                assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            }
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
        }
    }

    @Nested
    class WithDeadline {

        @Test
        void binds_an_internal_deadline_with_deferred_enforcement_when_there_is_none() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(5))) {
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
            try (DeadlineScope ignored =
                            external(Duration.ofSeconds(2), Enforcement.ENFORCE).attach();
                    DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(5))) {
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
            try (DeadlineScope ignored =
                            external(Duration.ofSeconds(2), Enforcement.ENFORCE).attach();
                    DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(2))) {
                clock.advance(Duration.ofSeconds(2));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }

        @Test
        void shortens_a_current_deadline_and_keeps_its_enforcement() {
            try (DeadlineScope ignored = external(Duration.ofSeconds(10), Enforcement.ENFORCE)
                            .attach();
                    DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.ENFORCE);

                clock.advance(Duration.ofSeconds(1));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void never_extends_the_current_deadline() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1));
                    DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
            }
        }

        @Test
        void extends_the_deadline_only_inside_without_deadline() {
            try (DeadlineScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    DeadlineScope ignored1 = Deadlines.withoutDeadline();
                    DeadlineScope ignored2 = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
                assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
            }
        }

        @ParameterizedTest
        @ValueSource(longs = {0, -1000})
        void binds_an_expired_deadline_for_a_zero_or_negative_timeout(long timeoutMillis) {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofMillis(timeoutMillis))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ZERO);
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void saturates_timeouts_too_long_to_represent_in_nanoseconds() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(Long.MAX_VALUE))) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofNanos(Long.MAX_VALUE));
            }
        }

        @Test
        void opening_and_closing_scopes_never_checks_the_deadline() {
            try (DeadlineScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));
                DeadlineContext expired = DeadlineContext.current();

                assertThatCode(() -> {
                            try (DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1));
                                    DeadlineScope ignored2 = Deadlines.withoutDeadline();
                                    DeadlineScope ignored3 = expired.attach()) {
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
            try (DeadlineScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(2));

                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    assertThat(Deadlines.getEnforcement()).isEmpty();
                    assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                            .doesNotThrowAnyException();
                }
            }
        }

        @Test
        void requests_carry_only_the_proposed_deadline_and_client_enforcement() {
            try (DeadlineScope ignored =
                            external(Duration.ofSeconds(1), Enforcement.ENFORCE).attach();
                    DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                assertThat(encode(Enforcement.DEFER)).containsOnly(entry(EXPECT_WITHIN, "300.000"));
                assertThat(encode(Enforcement.ENFORCE))
                        .containsOnly(entry(EXPECT_WITHIN, "300.000"), entry(EXPECT_WITHIN_ENFORCED, "true"));
            }
        }

        @Test
        void drops_the_enforcement_of_the_current_deadline() {
            DeadlineContext enforcementDisabled = DeadlineContext.fromRequest(
                    Optional.empty(),
                    Map.of(EXPECT_WITHIN, "1", EXPECT_WITHIN_ENFORCED, "false"),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
            try (DeadlineScope ignored = enforcementDisabled.attach()) {
                assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "false");

                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(encode(Enforcement.ENFORCE)).containsEntry(EXPECT_WITHIN_ENFORCED, "true");
                }
            }
        }
    }

    @Nested
    class ClosingScopes {

        @Test
        void closing_restores_the_context_that_was_current_when_the_scope_opened() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                assertNestedScopesRestore();
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
            }
            assertThat(DeadlineScope.currentBinding()).isNull();
        }

        private void assertNestedScopesRestore() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
            }
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // closes the scope on the wrong thread on purpose
        void closing_on_another_thread_does_nothing() throws Exception {
            DeadlineScope scope = Deadlines.withDeadline(Duration.ofSeconds(1));

            executor.submit(scope::close).get();

            assertThat(Deadlines.getRemainingDeadline())
                    .as("the scope is still open on this thread")
                    .contains(Duration.ofSeconds(1));
            assertThat(executor.submit(DeadlineScope::currentBinding).get())
                    .as("nothing is bound on the other thread")
                    .isNull();
            scope.close();
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // closes scopes out of order on purpose
        void closing_a_scope_also_closes_scopes_opened_after_it() {
            DeadlineScope outer = Deadlines.withDeadline(Duration.ofSeconds(10));
            DeadlineScope inner = Deadlines.withDeadline(Duration.ofSeconds(1));

            outer.close();
            assertThat(DeadlineScope.currentBinding()).isNull();

            inner.close();
            assertThat(DeadlineScope.currentBinding())
                    .as("closing the inner scope later does not bind the outer scope's context again")
                    .isNull();
        }

        @Test
        void closing_twice_does_nothing() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                try (DeadlineScope inner = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    inner.close();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
            }
        }
    }

    @Nested
    class HandOffs {

        @Test
        void a_wrapped_task_runs_with_the_context_that_was_current_when_it_was_wrapped() throws Exception {
            Callable<Optional<Duration>> task;
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                task = Deadlines.wrap(Deadlines::getRemainingDeadline);
            }
            clock.advance(Duration.ofMillis(300));

            assertThat(executor.submit(task).get())
                    .as("the deadline kept elapsing after the task was wrapped")
                    .contains(Duration.ofMillis(700));
        }

        @Test
        void a_wrapped_task_restores_the_running_threads_context_even_if_it_throws() {
            Runnable detached;
            try (DeadlineScope ignored = Deadlines.withoutDeadline()) {
                detached = Deadlines.wrap((Runnable) () -> {
                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    throw new SafeRuntimeException("task failed");
                });
            }

            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                assertThatThrownBy(detached::run).hasMessage("task failed");
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
            }
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // the task leaves its scope open on purpose
        void a_scope_left_open_by_a_wrapped_task_is_closed_when_the_task_ends() throws Exception {
            executor.submit(Deadlines.wrap((Runnable) () -> Deadlines.withDeadline(Duration.ofSeconds(1))))
                    .get();

            assertThat(executor.submit(DeadlineScope::currentBinding).get())
                    .as("nothing leaks into the next task on the same thread")
                    .isNull();
        }

        @Test
        void a_wrapped_executor_captures_the_context_when_a_task_is_submitted() throws Exception {
            ExecutorService wrapped = Deadlines.wrap(executor);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> blocker = executor.submit(() -> {
                release.await();
                return null;
            });

            Future<Optional<Duration>> submittedInScope;
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                submittedInScope = wrapped.submit(Deadlines::getRemainingDeadline);
            }
            Future<Optional<Duration>> submittedOutsideScope = wrapped.submit(Deadlines::getRemainingDeadline);
            release.countDown();
            blocker.get();

            assertThat(submittedInScope.get())
                    .as("runs after the scope closed, with the context captured at submission")
                    .contains(Duration.ofSeconds(1));
            assertThat(submittedOutsideScope.get()).isEmpty();
        }

        @Test
        void a_wrapped_executor_wraps_every_task_passed_to_invoke_all() throws Exception {
            ExecutorService wrapped = Deadlines.wrap(executor);
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                List<Future<Optional<Duration>>> results = wrapped.invokeAll(List.<Callable<Optional<Duration>>>of(
                        Deadlines::getRemainingDeadline, Deadlines::getRemainingDeadline));

                for (Future<Optional<Duration>> result : results) {
                    assertThat(result.get()).contains(Duration.ofSeconds(1));
                }
            }
        }

        @Test
        void unwrapped_hand_offs_do_not_carry_contexts() throws Exception {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.DEFER);

                try (DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    assertThat(executor.submit(Deadlines::getRemainingDeadline).get())
                            .as("a plain executor carries nothing")
                            .isEmpty();
                    assertThat(executor.submit(Tracers.wrap("test", Deadlines::getRemainingDeadline))
                                    .get())
                            .as("a tracing hand-off carries the trace, and with it only the trace state")
                            .contains(Duration.ofSeconds(5));
                    // The default executor of CompletableFuture's async methods.
                    assertThat(CompletableFuture.supplyAsync(Deadlines::getRemainingDeadline, ForkJoinPool.commonPool())
                                    .get())
                            .as("the common pool carries nothing")
                            .isEmpty();
                }
            }
        }

        @Test
        void a_context_bound_on_one_thread_never_affects_another_thread() throws Exception {
            ExecutorService wrapped = Deadlines.wrap(executor);
            ExecutorService otherWrapped = Deadlines.wrap(otherExecutor);
            CountDownLatch loadDetached = new CountDownLatch(1);
            CountDownLatch backgroundChecked = new CountDownLatch(1);

            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                Future<Optional<Duration>> backgroundSees = wrapped.submit(() -> {
                    loadDetached.await();
                    return Deadlines.getRemainingDeadline();
                });
                Future<Optional<Duration>> loadSees = otherWrapped.submit(() -> {
                    try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
                        loadDetached.countDown();
                        backgroundChecked.await();
                        return Deadlines.getRemainingDeadline();
                    }
                });

                assertThat(backgroundSees.get())
                        .as("background work keeps the request's deadline while the load is detached")
                        .contains(Duration.ofSeconds(1));
                backgroundChecked.countDown();
                assertThat(loadSees.get()).isEmpty();
                assertThat(Deadlines.getRemainingDeadline())
                        .as("the request thread keeps its deadline")
                        .contains(Duration.ofSeconds(1));
            }
        }

        @Test
        void callbacks_run_with_the_completing_threads_context() throws Exception {
            CompletableFuture<String> response = new CompletableFuture<>();
            CompletableFuture<Optional<Duration>> callbackSees;
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                callbackSees = response.thenApply(_value -> Deadlines.getRemainingDeadline());
            }

            executor.submit(() -> response.complete("value")).get();

            assertThat(callbackSees.get()).isEmpty();
        }

        @Test
        void a_captured_context_can_be_attached_in_a_callback() throws Exception {
            CompletableFuture<String> response = new CompletableFuture<>();
            CompletableFuture<Optional<Duration>> callbackSees;
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                DeadlineContext context = DeadlineContext.current();
                callbackSees = response.thenApply(_value -> {
                    try (DeadlineScope ignored1 = context.attach()) {
                        return Deadlines.getRemainingDeadline();
                    }
                });
            }

            executor.submit(() -> response.complete("value")).get();

            assertThat(callbackSees.get()).contains(Duration.ofSeconds(1));
        }
    }

    @Nested
    class CapturingContexts {

        @Test
        void a_captured_context_is_unaffected_by_later_scopes() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                DeadlineContext captured = DeadlineContext.current();

                try (DeadlineScope ignored1 = Deadlines.withoutDeadline();
                        DeadlineScope ignored2 = captured.attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                }
            }
        }

        @Test
        @SuppressWarnings("deprecation") // exercises disableFurtherDeadlinePropagation
        void capturing_trace_state_takes_a_snapshot() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.DEFER);
                DeadlineContext captured = DeadlineContext.current();

                Deadlines.disableFurtherDeadlinePropagation();

                assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                try (DeadlineScope ignored1 = captured.attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                }
            }
        }
    }

    @Nested
    class ParsingRequests {

        @Test
        void parsing_does_not_bind_the_context() {
            DeadlineContext parsed = external(Duration.ofSeconds(2), Enforcement.ENFORCE);

            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            try (DeadlineScope ignored = parsed.attach()) {
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(2));
            }
        }

        @Test
        void uses_the_shorter_of_the_header_and_internal_deadlines_preferring_the_header_on_a_tie() {
            DeadlineContext internalShorter = parse(Duration.ofSeconds(2), Optional.of(Duration.ofSeconds(1)));
            DeadlineContext headerShorter = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(2)));
            DeadlineContext tie = parse(Duration.ofSeconds(1), Optional.of(Duration.ofSeconds(1)));
            clock.advance(Duration.ofSeconds(1));

            assertExpired(internalShorter, DeadlineExpiredException.Internal.class);
            assertExpired(headerShorter, DeadlineExpiredException.External.class);
            assertExpired(tie, DeadlineExpiredException.External.class);
        }

        @Test
        void has_no_deadline_without_a_header_or_an_internal_deadline() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);
                DeadlineContext parsed = DeadlineContext.fromRequest(
                        Optional.empty(), Map.of(), MapAdapter.INSTANCE, Enforcement.ENFORCE);

                try (DeadlineScope ignored1 = parsed.attach()) {
                    assertThat(Deadlines.getRemainingDeadline())
                            .as("a request without a deadline does not inherit the trace state")
                            .isEmpty();
                }
            }
        }

        private DeadlineContext parse(Duration header, Optional<Duration> internalDeadline) {
            return DeadlineContext.fromRequest(
                    internalDeadline,
                    Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(header.toNanos())),
                    MapAdapter.INSTANCE,
                    Enforcement.ENFORCE);
        }

        private void assertExpired(DeadlineContext context, Class<? extends DeadlineExpiredException> expected) {
            try (DeadlineScope ignored = context.attach()) {
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(expected);
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
            try (DeadlineScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DEFER).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.DEFER))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void disabled_enforcement_is_never_enforced() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (DeadlineScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DISABLE).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.ENFORCE))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get()");
        }

        @Test
        void client_enforcement_enforces_a_deferred_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (DeadlineScope ignored =
                    external(Duration.ofSeconds(1), Enforcement.DEFER).attach()) {
                assertThat(Deadlines.awaitWithinDeadline(future, Enforcement.ENFORCE))
                        .isEqualTo("value");
            }
            assertThat(future.calls).containsExactly("get(1000000000ns)");
        }

        @Test
        void waits_at_most_the_remaining_time_of_an_enforced_deadline() throws Exception {
            RecordingFuture future = RecordingFuture.pending();
            try (DeadlineScope ignored =
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
            try (DeadlineScope ignored =
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
            try (DeadlineScope ignored =
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
            try (DeadlineScope ignored =
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
            try (DeadlineScope ignored =
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
            ExecutorService requestScopedExecutor = Deadlines.wrap(otherExecutor);

            // Caller 1 has 50ms left and misses, so it starts the load. The cache's executor here propagates the
            // caller's deadline, to show that the load's own scope is what removes it.
            Future<String> load;
            try (DeadlineScope ignored =
                    external(Duration.ofMillis(50), Enforcement.ENFORCE).attach()) {
                load = Deadlines.wrap(executor).submit(() -> {
                    try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
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
                try (DeadlineScope ignored =
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
        void disabling_propagation_on_a_thread_with_a_scope_lasts_until_the_innermost_scope_closes() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(10))) {
                try (DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    Deadlines.disableFurtherDeadlinePropagation();

                    assertThat(Deadlines.getRemainingDeadline()).isEmpty();
                    assertThat(encode(Enforcement.ENFORCE))
                            .as("no deadline headers, as for disabled trace state")
                            .isEmpty();
                }
                assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(10));
            }
        }

        @Test
        void disabling_propagation_does_not_affect_captured_contexts() {
            try (DeadlineScope ignored = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                DeadlineContext captured = DeadlineContext.current();

                Deadlines.disableFurtherDeadlinePropagation();

                try (DeadlineScope ignored1 = captured.attach()) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(1));
                }
            }
        }

        @Test
        void disabling_propagation_on_a_thread_with_a_scope_also_disables_trace_state() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                setTraceDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(1))) {
                    Deadlines.disableFurtherDeadlinePropagation();
                }

                assertThat(Deadlines.getRemainingDeadline()).isEmpty();
            }
        }

        @Test
        void parsing_into_trace_state_is_not_visible_to_a_thread_with_a_scope() {
            try (CloseableTracer ignored = CloseableTracer.startSpan("test")) {
                try (DeadlineScope ignored1 = Deadlines.withoutDeadline()) {
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

                try (DeadlineScope ignored1 = Deadlines.withDeadline(Duration.ofSeconds(5))) {
                    assertThat(Deadlines.getRemainingDeadline()).contains(Duration.ofSeconds(5));
                    assertThat(Deadlines.getEnforcement()).contains(Enforcement.DEFER);
                }
            }
        }
    }

    // A context with an external deadline, as parsed from an inbound request.
    private static DeadlineContext external(Duration deadline, Enforcement enforcement) {
        return DeadlineContext.fromRequest(
                Optional.empty(),
                Map.of(EXPECT_WITHIN, Deadlines.durationToHeaderValue(deadline.toNanos())),
                MapAdapter.INSTANCE,
                enforcement);
    }

    // A context with an internal deadline, as configured for an endpoint.
    private static DeadlineContext internal(Duration deadline, Enforcement enforcement) {
        return DeadlineContext.fromRequest(Optional.of(deadline), Map.of(), MapAdapter.INSTANCE, enforcement);
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
