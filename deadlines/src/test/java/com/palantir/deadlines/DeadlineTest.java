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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.groups.Tuple.tuple;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.palantir.deadlines.Deadline.Origin;
import com.palantir.deadlines.DeadlineMetrics.Received_Enforcement;
import com.palantir.deadlines.DeadlineMetrics.Received_Origin;
import com.palantir.deadlines.DeadlineMetrics.Received_State;
import com.palantir.deadlines.DeadlineMetrics.Revoked_Reason;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.logsafe.Arg;
import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Detached;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DeadlineTest {

    private static final Map<String, String> NO_HEADERS = Map.of();
    private static final HeaderReader<Map<String, String>> READER = Map::get;
    private static final HeaderWriter<Map<String, String>> WRITER = Map::put;

    private final TestClock clock = new TestClock();

    @BeforeEach
    void freezeClock() {
        Deadlines.setClock(clock);
    }

    @AfterEach
    void restoreClock() {
        Deadlines.resetClock();
    }

    @Nested
    class Handle {

        @Test
        void remaining_is_recomputed_rather_than_frozen_at_the_moment_of_the_read() {
            // The reason a handle is returned instead of a Duration. A caller that holds a duration has something
            // that silently becomes wrong, and may reuse it as though it were a fresh budget.
            Deadline deadline = Deadline.of(Duration.ofSeconds(10));
            assertThat(deadline.remaining()).isEqualTo(Duration.ofSeconds(10));

            clock.elapsed += Duration.ofSeconds(3).toNanos();

            assertThat(deadline.remaining()).isEqualTo(Duration.ofSeconds(7));
        }

        @Test
        void remaining_is_clamped_at_zero_once_the_budget_has_run_out() {
            Deadline deadline = Deadline.of(Duration.ofSeconds(1));
            clock.elapsed += Duration.ofSeconds(5).toNanos();

            assertThat(deadline.remaining()).isEqualTo(Duration.ZERO);
            assertThat(deadline.isExpired()).isTrue();
        }

        @Test
        void a_handle_answers_on_a_thread_with_no_trace_attached() throws Exception {
            Deadline captured;
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                captured = establish(Duration.ofSeconds(5), Enforcement.ENFORCE);
            }

            assertThat(Deadlines.current())
                    .as("the trace has ended, so there is no deadline to read from trace state")
                    .isEmpty();

            AtomicReference<Duration> remainingOffTrace = new AtomicReference<>();
            Thread other = new Thread(() -> remainingOffTrace.set(captured.remaining()));
            other.start();
            other.join();

            assertThat(remainingOffTrace.get()).isEqualTo(Duration.ofSeconds(5));
        }

        @Test
        void has_passed_reports_the_budget_alone_while_is_expired_reports_whether_it_still_applies() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Deadline deadline = establish(Duration.ofSeconds(1), Enforcement.ENFORCE);
                clock.elapsed += Duration.ofSeconds(2).toNanos();
                assertThat(deadline.isExpired()).isTrue();
                assertThat(deadline.hasPassed()).isTrue();

                Deadlines.revoke();

                assertThat(deadline.isExpired())
                        .as("a revoked deadline constrains nothing, so nothing should stop for it")
                        .isFalse();
                assertThat(deadline.hasPassed())
                        .as("the request still overran its budget, which is what reporting cares about")
                        .isTrue();
            }
        }

        @Test
        void to_string_describes_the_budget_and_whether_it_still_applies() {
            Deadline deadline = Deadline.of(Duration.ofSeconds(5), Origin.EXTERNAL, Enforcement.ENFORCE);
            assertThat(deadline.toString())
                    .isEqualTo("Deadline{budget=PT5S, remaining=PT5S, origin=EXTERNAL, enforcement=ENFORCE}");

            deadline.revoke();

            assertThat(deadline.toString()).endsWith(", revoked}");
        }
    }

    @Nested
    class EnforcementResolution {

        @ParameterizedTest(name = "trace {0} with client {1} binds: {2}")
        @CsvSource({
            "ENFORCE,ENFORCE,true",
            "ENFORCE,DEFER,true",
            "ENFORCE,DISABLE,false",
            "DEFER,ENFORCE,true",
            "DEFER,DEFER,false",
            "DEFER,DISABLE,false",
            "DISABLE,ENFORCE,false",
            "DISABLE,DEFER,false",
            "DISABLE,DISABLE,false",
        })
        void binds_under_covers_every_pair_of_strategies(
                Enforcement traceEnforcement, Enforcement clientEnforcement, boolean expected) {
            Deadline deadline = Deadline.of(Duration.ofSeconds(5), Origin.EXTERNAL, traceEnforcement);

            assertThat(deadline.bindsUnder(clientEnforcement)).isEqualTo(expected);
        }

        @Test
        void a_deferring_server_called_through_an_enforcing_client_is_bound() {
            // The case a caller comparing enforcement() to ENFORCE by hand gets wrong, and the reason resolution
            // lives on the handle rather than being left to each consumer.
            Deadline deadline = Deadline.of(Duration.ofSeconds(5), Origin.EXTERNAL, Enforcement.DEFER);

            assertThat(deadline.enforcement()).isNotEqualTo(Enforcement.ENFORCE);
            assertThat(deadline.bindsUnder(Enforcement.ENFORCE)).isTrue();
        }

        @Test
        void a_revoked_deadline_binds_nobody() {
            Deadline deadline = Deadline.of(Duration.ofSeconds(5), Origin.EXTERNAL, Enforcement.ENFORCE);
            deadline.revoke();

            assertThat(deadline.bindsUnder(Enforcement.ENFORCE)).isFalse();
        }

        @Test
        void check_throws_only_when_expired_and_binding() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Deadline deadline = establish(Duration.ofSeconds(1), Enforcement.DEFER);

                assertThatCode(() -> deadline.check(Enforcement.ENFORCE))
                        .as("not expired yet")
                        .doesNotThrowAnyException();

                clock.elapsed += Duration.ofSeconds(2).toNanos();

                assertThatCode(() -> deadline.check(Enforcement.DEFER))
                        .as("expired, but nobody is enforcing")
                        .doesNotThrowAnyException();
                assertThatExceptionOfType(DeadlineExpiredException.Internal.class)
                        .isThrownBy(() -> deadline.check(Enforcement.ENFORCE));
            }
        }
    }

    @Nested
    class Revocation {

        @Test
        void revocation_reaches_a_handle_captured_on_another_thread() throws Exception {
            // This is what lets a consumer that captured a handle find out the deadline stopped applying without
            // re-reading trace state, which it cannot do from a timer or scheduler thread at all.
            Deadline captured;
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                captured = establish(Duration.ofSeconds(5), Enforcement.ENFORCE);
                Deadlines.revoke();
            }

            AtomicReference<Boolean> revokedOffTrace = new AtomicReference<>();
            Thread other = new Thread(() -> revokedOffTrace.set(captured.isRevoked()));
            other.start();
            other.join();

            assertThat(revokedOffTrace.get()).isTrue();
        }

        @Test
        void revoking_twice_is_recorded_once() {
            Meter meter = metrics().revoked(Revoked_Reason.CALLER);
            long before = meter.getCount();

            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(5), Enforcement.ENFORCE);
                Deadlines.revoke();
                Deadlines.revoke();
            }

            assertThat(meter.getCount()).isEqualTo(before + 1);
        }

        @Test
        void request_completed_records_the_remaining_budget_and_revokes() {
            Histogram headroom = metrics().headroom();
            Meter revoked = metrics().revoked(Revoked_Reason.REQUEST_COMPLETED);
            long headroomBefore = headroom.getCount();
            long revokedBefore = revoked.getCount();

            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(10), Enforcement.ENFORCE);
                clock.elapsed += Duration.ofSeconds(4).toNanos();

                Optional<Deadline> completed = Deadlines.requestCompleted();

                assertThat(completed).isPresent();
                assertThat(completed.orElseThrow().isRevoked()).isTrue();
            }

            assertThat(headroom.getCount()).isEqualTo(headroomBefore + 1);
            assertThat(headroom.getSnapshot().getMax())
                    .isEqualTo(Duration.ofSeconds(6).toMillis());
            assertThat(revoked.getCount()).isEqualTo(revokedBefore + 1);
        }

        @Test
        void request_completed_is_a_noop_when_no_deadline_was_established() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                assertThat(Deadlines.requestCompleted()).isEmpty();
            }
        }
    }

    @Nested
    class Scopes {

        @Test
        void attach_establishes_a_deadline_and_restores_the_previous_one_on_close() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Deadline original = establish(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (CloseableDeadlineScope scope = Deadlines.attach(Deadline.of(Duration.ofMinutes(1)))) {
                    assertThat(Deadlines.current().orElseThrow().remaining()).isEqualTo(Duration.ofMinutes(1));
                }

                assertThat(Deadlines.current()).hasValue(original);
            }
        }

        @Test
        void attach_removes_the_deadline_again_when_there_was_none_to_restore() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                try (CloseableDeadlineScope scope = Deadlines.attach(Deadline.of(Duration.ofMinutes(1)))) {
                    assertThat(Deadlines.current()).isPresent();
                }

                assertThat(Deadlines.current()).isEmpty();
            }
        }

        @Test
        void attach_grants_a_fresh_budget_to_work_whose_deadline_had_run_out() {
            // Replaces having to revoke the deadline for the rest of the request just to let a final write through.
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(1), Enforcement.ENFORCE);
                clock.elapsed += Duration.ofSeconds(2).toNanos();

                Map<String, String> outbound = new HashMap<>();
                try (CloseableDeadlineScope scope = Deadlines.attach(Deadline.of(Duration.ofMinutes(1)))) {
                    assertThatCode(() ->
                                    Deadlines.toRequest(outbound, WRITER, Duration.ofSeconds(30), Enforcement.ENFORCE))
                            .doesNotThrowAnyException();
                }

                assertThat(outbound).containsEntry(DeadlinesHttpHeaders.EXPECT_WITHIN, "30.000");
                assertThatExceptionOfType(DeadlineExpiredException.class)
                        .as("the original deadline is back in force outside the scope")
                        .isThrownBy(() -> Deadlines.toRequest(
                                new HashMap<>(), WRITER, Duration.ofSeconds(30), Enforcement.ENFORCE));
            }
        }

        @Test
        void attach_is_visible_to_other_threads_in_the_same_trace() throws Exception {
            // Unlike detach, which must not be, because work handed off from inside an attach scope is exactly the
            // work that needs the replacement budget.
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(5), Enforcement.ENFORCE);

                AtomicReference<Duration> seenOnOtherThread = new AtomicReference<>();
                try (CloseableDeadlineScope scope = Deadlines.attach(Deadline.of(Duration.ofMinutes(1)))) {
                    Detached sameTrace = DetachedSpan.detach();
                    Thread other = new Thread(() -> seenOnOtherThread.set(remainingIn(sameTrace)));
                    other.start();
                    other.join();
                }

                assertThat(seenOnOtherThread.get()).isEqualTo(Duration.ofMinutes(1));
            }
        }

        @Test
        void detach_hides_the_deadline_and_is_counted() {
            Meter detachedMeter = metrics().detached();
            long before = detachedMeter.getCount();

            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(5), Enforcement.ENFORCE);

                try (CloseableDeadlineScope scope = Deadlines.detach()) {
                    assertThat(Deadlines.current()).isEmpty();
                }

                assertThat(Deadlines.current()).isPresent();
            }

            assertThat(detachedMeter.getCount())
                    .as("suppressing a deadline should leave evidence that it happened")
                    .isEqualTo(before + 1);
        }
    }

    @Nested
    class OriginSelection {

        @Test
        void the_received_deadline_wins_ties_with_the_servers_own_budget() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Map<String, String> request = Map.of(
                        DeadlinesHttpHeaders.EXPECT_WITHIN,
                        ExpectWithinHeader.format(Duration.ofSeconds(5).toNanos()));

                Deadline deadline = Deadlines.fromRequest(
                                request, READER, Optional.of(Duration.ofSeconds(5)), Enforcement.DEFER)
                        .orElseThrow();

                assertThat(deadline.origin()).isEqualTo(Origin.EXTERNAL);
            }
        }

        @Test
        void the_servers_own_budget_binds_when_it_is_smaller() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Map<String, String> request = Map.of(
                        DeadlinesHttpHeaders.EXPECT_WITHIN,
                        ExpectWithinHeader.format(Duration.ofSeconds(30).toNanos()));

                Deadline deadline = Deadlines.fromRequest(
                                request, READER, Optional.of(Duration.ofSeconds(5)), Enforcement.DEFER)
                        .orElseThrow();

                assertThat(deadline.origin()).isEqualTo(Origin.INTERNAL);
                assertThat(deadline.remaining()).isEqualTo(Duration.ofSeconds(5));
            }
        }

        @Test
        void a_callers_own_smaller_proposal_expires_as_internal_even_against_an_external_deadline() {
            // The origin of an expiration follows whichever budget actually bound, so an exhausted budget of this
            // process' own is never attributed to the caller.
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Map<String, String> request = Map.of(
                        DeadlinesHttpHeaders.EXPECT_WITHIN,
                        ExpectWithinHeader.format(Duration.ofSeconds(30).toNanos()));
                Deadlines.fromRequest(request, READER, Optional.empty(), Enforcement.ENFORCE);

                assertThatExceptionOfType(DeadlineExpiredException.Internal.class)
                        .isThrownBy(
                                () -> Deadlines.toRequest(new HashMap<>(), WRITER, Duration.ZERO, Enforcement.ENFORCE));
            }
        }

        @Test
        void establishing_a_deadline_is_recorded_with_its_origin_and_enforcement() {
            Meter received = metrics()
                    .received()
                    .origin(Received_Origin.EXTERNAL)
                    .enforcement(Received_Enforcement.ENFORCE)
                    .state(Received_State.LIVE)
                    .build();
            long before = received.getCount();

            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Map<String, String> request = Map.of(
                        DeadlinesHttpHeaders.EXPECT_WITHIN,
                        ExpectWithinHeader.format(Duration.ofSeconds(5).toNanos()));
                Deadlines.fromRequest(request, READER, Optional.empty(), Enforcement.ENFORCE);
            }

            assertThat(received.getCount())
                    .as("without this there is no denominator for the expiration meter")
                    .isEqualTo(before + 1);
        }

        @Test
        void a_deadline_that_arrives_already_expired_is_recorded_as_such() {
            Meter expiredOnArrival = metrics()
                    .received()
                    .origin(Received_Origin.EXTERNAL)
                    .enforcement(Received_Enforcement.DEFER)
                    .state(Received_State.EXPIRED_ON_ARRIVAL)
                    .build();
            long before = expiredOnArrival.getCount();

            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                Map<String, String> request = Map.of(DeadlinesHttpHeaders.EXPECT_WITHIN, "0");

                Deadline deadline = Deadlines.fromRequest(request, READER, Optional.empty(), Enforcement.DEFER)
                        .orElseThrow();

                assertThat(deadline.hasPassed()).isTrue();
            }

            assertThat(expiredOnArrival.getCount()).isEqualTo(before + 1);
        }

        @Test
        void no_deadline_is_established_when_neither_a_header_nor_a_server_budget_is_present() {
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                assertThat(Deadlines.fromRequest(NO_HEADERS, READER, Optional.empty(), Enforcement.ENFORCE))
                        .isEmpty();
                assertThat(Deadlines.current()).isEmpty();
            }
        }
    }

    @Nested
    class Exceptions {

        @Test
        void an_expiration_reports_the_budget_and_how_far_past_it_the_request_ran() {
            // Expiration does not interrupt work, so the elapsed time is not the budget: it says how long the
            // request had really been running by the time anything noticed.
            try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
                establish(Duration.ofSeconds(5), Enforcement.ENFORCE);
                clock.elapsed += Duration.ofSeconds(8).toNanos();

                DeadlineExpiredException exception =
                        Deadlines.current().orElseThrow().expire();

                assertThat(exception.getArgs())
                        .extracting(Arg::getName, Arg::getValue)
                        .containsExactly(
                                tuple("deadlineMillis", 5_000L),
                                tuple("elapsedMillis", 8_000L),
                                tuple("origin", Origin.INTERNAL));
            }
        }

        @Test
        void the_public_factories_still_produce_argument_free_exceptions() {
            // Used when reconstructing an exception from a response, where no budget is known.
            assertThat(DeadlineExpiredException.external().getArgs()).isEmpty();
            assertThat(DeadlineExpiredException.internal().getArgs()).isEmpty();
        }
    }

    @Nested
    class HeaderCodec {

        @Test
        void format_and_parse_round_trip_at_millisecond_precision() {
            for (long millis : new long[] {1L, 7L, 250L, 999L, 1_000L, 5_500L, 60_000L, 3_600_000L}) {
                long nanos = Duration.ofMillis(millis).toNanos();
                assertThat(ExpectWithinHeader.parse(ExpectWithinHeader.format(nanos)))
                        .as("round trip of %sms", millis)
                        .isEqualTo(nanos);
            }
        }

        @Test
        void format_rounds_up_to_the_next_millisecond_so_budget_is_not_lost_per_hop() {
            // A deep call chain that propagates quickly would otherwise shed most of a millisecond at every hop.
            assertThat(ExpectWithinHeader.format(1L)).isEqualTo("0.001");
            assertThat(ExpectWithinHeader.format(1_000_001L)).isEqualTo("0.002");
        }

        @Test
        void format_never_emits_a_negative_value() {
            assertThat(ExpectWithinHeader.format(-1L)).isEqualTo("0");
            assertThat(ExpectWithinHeader.format(0L)).isEqualTo("0");
        }
    }

    /** Reads the visible deadline's remaining time while attached to the given trace. */
    @Nullable
    private static Duration remainingIn(Detached trace) {
        try (CloseableSpan child = trace.childSpan("other")) {
            return Deadlines.current().map(Deadline::remaining).orElse(null);
        }
    }

    private static Deadline establish(Duration budget, Enforcement enforcement) {
        return Deadlines.fromRequest(NO_HEADERS, READER, Optional.of(budget), enforcement)
                .orElseThrow();
    }

    @SuppressWarnings("for-rollout:deprecation")
    private static DeadlineMetrics metrics() {
        return DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
    }

    private static final class TestClock implements Deadlines.Clock {
        private long elapsed = 0L;

        @Override
        public long nanoTime() {
            return elapsed;
        }
    }
}
