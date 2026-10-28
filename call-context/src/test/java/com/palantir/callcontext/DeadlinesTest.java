/*
 * (c) Copyright 2025 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.lists;

import com.codahale.metrics.Meter;
import com.palantir.callcontext.DeadlineMetrics.Expired_Budget;
import com.palantir.callcontext.DeadlineMetrics.Expired_Cause;
import com.palantir.callcontext.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.DeadlineExpiredException;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.DeadlinesHttpHeaders;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

class DeadlinesTest {

    @AfterEach
    void afterEach() {
        Deadlines.setClock(System::nanoTime);
    }

    @Test
    public void test_duration_to_header_value_avoids_encoding_negative_values() {
        long duration = Duration.ofMillis(-2).toNanos();
        String headerValue = Deadlines.durationToHeaderValue(duration);
        assertThat(headerValue).isEqualTo("0");
    }

    @Test
    public void test_duration_to_header_value_ceiling_on_millis() {
        assertThat(Deadlines.durationToHeaderValue(1)).isEqualTo("0.001");
        assertThat(Deadlines.durationToHeaderValue(1000001)).isEqualTo("0.002");
        assertThat(Deadlines.durationToHeaderValue(1999999)).isEqualTo("0.002");
        assertThat(Deadlines.durationToHeaderValue(9999999)).isEqualTo("0.010");
        assertThat(Deadlines.durationToHeaderValue(10000001)).isEqualTo("0.011");
        assertThat(Deadlines.durationToHeaderValue(19999999)).isEqualTo("0.020");
        assertThat(Deadlines.durationToHeaderValue(99999999)).isEqualTo("0.100");
        assertThat(Deadlines.durationToHeaderValue(1000000001)).isEqualTo("1.001");
        assertThat(Deadlines.durationToHeaderValue(1999999999)).isEqualTo("2.000");
    }

    @Test
    public void test_duration_to_header_value_avoids_overflow() {
        long duration = Long.MAX_VALUE;
        long expected = 9223372036853999616L;
        String headerValue = Deadlines.durationToHeaderValue(duration);
        Long parsed = Deadlines.tryParseSecondsToNanoseconds(headerValue);
        assertThat(parsed).isEqualTo(expected);
    }

    @Test
    public void test_duration_to_header_value() {
        long duration = Duration.ofMillis(1523).toNanos();
        String headerValue = Deadlines.durationToHeaderValue(duration);
        assertThat(headerValue).isEqualTo("1.523");
    }

    @ParameterizedTest
    @CsvSource({
        "0, 0",
        "0.0, 0",
        "1, 1000000000",
        "01, 1000000000",
        "09, 9000000000",
        "1.0, 1000000000",
        "1.00000, 1000000000",
        " 123.4567890246, 123456789024",
        "' 2. ', 2000000000",
        "3.0, 3000000000",
        "3.1, 3100000000",
        "1234567890.12345, 1234567890123450112",
        "1.523, 1523000000",
        "'   1.523  ', 1523000000",
        "1234567890123467890123467890123467890123467890123467890123467890, 9223372036854775807",
        "12345678901234678901234678901234678901234678901234678901234678901.123467890123467890, 9223372036854775807",
    })
    public void test_header_value_to_duration(String input, long expectedNanos) {
        assertThat(Deadlines.tryParseSecondsToNanoseconds(input))
                .isNotNull()
                .isEqualTo(expectedNanos)
                .isEqualTo((long) (Double.parseDouble(input) * 1_000_000_000.0));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                " ",
                ".",
                "foo",
                "1,234",
                "1.2.3",
                "1 2",
                "-123",
                "1.234e5",
                "1.23E4",
                "NaN",
                "Infinity",
                "0x123"
            })
    public void test_invalid_header_value_to_duration(String headerValue) {
        assertThat(Deadlines.tryParseSecondsToNanoseconds(headerValue)).isNull();
    }

    @Test
    void check_tryParseSecondsToNanoseconds_successfully_parses_numeric_values() {
        Gen<String> numericStringGen = stringGen("0123456789", 1, 100);
        qt().withExamples(100_000).forAll(numericStringGen, numericStringGen).checkAssert((integer, decimal) -> {
            assertThat(Deadlines.tryParseSecondsToNanoseconds(integer))
                    .isNotNull()
                    .isGreaterThanOrEqualTo(0)
                    .isLessThanOrEqualTo(Long.MAX_VALUE);
            assertThat(Deadlines.tryParseSecondsToNanoseconds(decimal))
                    .isNotNull()
                    .isGreaterThanOrEqualTo(0)
                    .isLessThanOrEqualTo(Long.MAX_VALUE);
            assertThat(Deadlines.tryParseSecondsToNanoseconds(integer + '.' + decimal))
                    .isNotNull()
                    .isGreaterThanOrEqualTo(0)
                    .isLessThanOrEqualTo(Long.MAX_VALUE);
        });
    }

    private static Gen<String> stringGen(String validChars, int minLength, int maxLength) {
        Gen<Character> characterGen =
                Generate.pick(validChars.chars().mapToObj(c -> (char) c).toList());
        return lists().of(characterGen).ofSizeBetween(minLength, maxLength).map(chars -> {
            StringBuilder sb = new StringBuilder(chars.size());
            chars.forEach(sb::append);
            return sb.toString();
        });
    }

    @Test
    public void can_encode_to_request() {
        Map<String, String> request = new HashMap<>();
        Duration deadline = Duration.ofSeconds(1);
        Deadlines.encodeToRequest(deadline, request, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

        assertThat(Optional.ofNullable(request.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                .hasValueSatisfying(s -> {
                    String expected = Deadlines.durationToHeaderValue(deadline.toNanos());
                    assertThat(s).isEqualTo(expected);
                });
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_internal_state() {
        Map<String, String> inboundRequest = new HashMap<>();
        long originalDeadline = Duration.ofSeconds(1).toNanos();
        inboundRequest.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(originalDeadline));
        try (CallContextScope ignored =
                parse(Optional.empty(), inboundRequest, Enforcement.DISABLE).attach()) {
            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(2);
            Deadlines.encodeToRequest(
                    providedDeadline, outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                    .hasValueSatisfying(h -> {
                        Long parsed = Deadlines.tryParseSecondsToNanoseconds(h);
                        assertThat(parsed).isNotNull().isLessThanOrEqualTo(originalDeadline);
                    });
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_argument() {
        Map<String, String> inboundRequest = new HashMap<>();
        inboundRequest.put(
                DeadlinesHttpHeaders.EXPECT_WITHIN,
                Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), inboundRequest, Enforcement.DISABLE).attach()) {
            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(
                    providedDeadline, outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                    .hasValueSatisfying(h -> {
                        Long parsed = Deadlines.tryParseSecondsToNanoseconds(h);
                        assertThat(parsed).isLessThanOrEqualTo(providedDeadline.toNanos());
                    });
        }
    }

    @Test
    public void test_encode_to_request_expiration_external_deadline() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.DISABLE).attach()) {
            clock.advance(Duration.ofMillis(2));

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));

            @SuppressWarnings("for-rollout:deprecation")
            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .budget(Expired_Budget.SUB_100MS)
                    .build();
            Meter internalMeter = metrics.expired()
                    .cause(Expired_Cause.INTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .budget(Expired_Budget.SUB_100MS)
                    .build();
            long originalExternalValue = externalMeter.getCount();
            long originalInternalValue = internalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(externalMeter.getCount()).isGreaterThan(originalExternalValue);
            assertThat(internalMeter.getCount()).isEqualTo(originalInternalValue);
        }
    }

    @Test
    public void test_encode_to_request_expiration_internal_deadline() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(100);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        try (CallContextScope ignored = parse(Optional.of(Duration.ofMillis(1)), request, Enforcement.DISABLE)
                .attach()) {
            clock.advance(Duration.ofMillis(2));
            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));

            @SuppressWarnings("for-rollout:deprecation")
            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .budget(Expired_Budget.SUB_100MS)
                    .build();
            Meter internalMeter = metrics.expired()
                    .cause(Expired_Cause.INTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .budget(Expired_Budget.SUB_100MS)
                    .build();
            long originalExternalValue = externalMeter.getCount();
            long originalInternalValue = internalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(internalMeter.getCount()).isGreaterThan(originalInternalValue);
            assertThat(externalMeter.getCount()).isEqualTo(originalExternalValue);
        }
    }

    @Test
    public void multihop_expired_received_deadline_marks_propagate_already_expired_meter() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);

        @SuppressWarnings("for-rollout:deprecation")
        DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
        Meter expiredMeterPropagateIntent = metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE)
                .budget(Expired_Budget.SUB_100MS)
                .build();
        Meter expiredMeterPropagateAlreadyExpiredIntent = metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE_ALREADY_EXPIRED)
                .budget(Expired_Budget.SUB_100MS)
                .build();

        long expiredMeterPropagateIntentValue = expiredMeterPropagateIntent.getCount();
        long expiredMeterPropagateAlreadyExpiredIntentValue = expiredMeterPropagateAlreadyExpiredIntent.getCount();

        // the first hop receives a valid, non-zero deadline on the wire
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.DISABLE).attach()) {
            // nothing yet...
            assertThat(expiredMeterPropagateIntent.getCount()).isEqualTo(expiredMeterPropagateIntentValue);
            assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount())
                    .isEqualTo(expiredMeterPropagateAlreadyExpiredIntentValue);

            // force expiration within the first hop
            clock.advance(Duration.ofMillis(2));

            Map<String, String> outbound1 = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound1, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            // this hop marks the meter with the "propagate" intent, as the expiration happened here
            assertThat(outbound1.get(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .isNotNull()
                    .isEqualTo("0");
            assertThat(expiredMeterPropagateIntent.getCount()).isGreaterThan(expiredMeterPropagateIntentValue);
            assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount()).isZero();

            // next hop parses a zero deadline
            expiredMeterPropagateIntentValue = expiredMeterPropagateIntent.getCount();
            try (CallContextScope ignored2 =
                    parse(Optional.empty(), outbound1, Enforcement.DISABLE).attach()) {
                // sending another request when the deadline has already expired should
                // mark the meter with the "propagate-already-expired" intent
                expiredMeterPropagateAlreadyExpiredIntentValue = expiredMeterPropagateAlreadyExpiredIntent.getCount();
                Map<String, String> outbound2 = new HashMap<>();
                Deadlines.encodeToRequest(
                        Duration.ofSeconds(10), outbound2, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);
                assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount())
                        .isGreaterThan(expiredMeterPropagateAlreadyExpiredIntentValue);
                // meter with the "propagate" intent is unchanged
                assertThat(expiredMeterPropagateIntent.getCount()).isEqualTo(expiredMeterPropagateIntentValue);
            }
        }
    }

    @Test
    public void test_enforced_deadline_expiration_reports_throw_intent_metric() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.ENFORCE).attach()) {
            clock.advance(Duration.ofMillis(2));

            // additionally validate that the expired deadline meter is marked with the "THROW" intent
            @SuppressWarnings("for-rollout:deprecation")
            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.THROW)
                    .budget(Expired_Budget.SUB_100MS)
                    .build();
            long originalExternalValue = externalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            assertThatThrownBy(() -> Deadlines.encodeToRequest(
                            Duration.ofSeconds(5), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER))
                    .isInstanceOf(DeadlineExpiredException.External.class);
            assertThat(externalMeter.getCount()).isGreaterThan(originalExternalValue);
        }
    }

    @Test
    public void test_external_deadline_expiration_is_enforced_by_external_flag() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "true");
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.DEFER).attach()) {
            clock.advance(Duration.ofMillis(2));

            Map<String, String> outbound = new HashMap<>();
            assertThatThrownBy(() -> Deadlines.encodeToRequest(
                            Duration.ofSeconds(5), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER))
                    .isInstanceOf(DeadlineExpiredException.External.class);
        }
    }

    @Test
    public void test_external_deadline_expiration_is_not_enforced_when_external_flag_is_not_true() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofMillis(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "false");
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.ENFORCE).attach()) {
            clock.advance(Duration.ofMillis(2));

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(5), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);
            assertThat(outbound).containsEntry(DeadlinesHttpHeaders.EXPECT_WITHIN, "0");
            assertThat(outbound).containsEntry(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "false");
        }
    }

    @Test
    public void test_enforcement_flag_is_propagated_and_enforced_at_next_hop() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);

        Map<String, String> inbound1 = new HashMap<>();
        Duration providedDeadline = Duration.ofSeconds(1);
        inbound1.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), inbound1, Enforcement.ENFORCE).attach()) {
            clock.advance(Duration.ofMillis(500));

            Map<String, String> outbound1 = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound1, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);
            assertThat(outbound1).containsEntry(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "true");

            try (CallContextScope ignored2 =
                    parse(Optional.empty(), outbound1, Enforcement.DEFER).attach()) {
                clock.advance(Duration.ofMillis(600));

                Map<String, String> outbound2 = new HashMap<>();
                assertThatThrownBy(() -> Deadlines.encodeToRequest(
                                Duration.ofSeconds(10), outbound2, DummyRequestEncoder.INSTANCE, Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }
    }

    @ParameterizedTest(name = "deadline={0}, header={1}, local={2} -> {3}")
    @CsvSource({
        "1, , DEFER, DEFER,",
        "1, , ENFORCE, ENFORCE, true",
        "1, , DISABLE, DISABLE, false",
        "1, true, DEFER, ENFORCE, true",
        "1, false, ENFORCE, DISABLE, false",
        "1, false, DEFER, DISABLE, false",
        ", false, ENFORCE, ENFORCE, true",
        ", true, DEFER, DEFER,",
        ", true, DISABLE, DISABLE, false",
        "1, foobar, ENFORCE, ENFORCE, true",
        "1, foobar, DEFER, DEFER,",
        "1, foobar, DISABLE, DISABLE, false",
    })
    void resolves_and_propagates_request_enforcement(
            @Nullable String deadlineHeader,
            @Nullable String enforcementHeader,
            Enforcement localEnforcement,
            Enforcement expectedEnforcement,
            @Nullable String expectedHeader) {
        Map<String, String> inbound = new HashMap<>();
        if (deadlineHeader != null) {
            inbound.put(DeadlinesHttpHeaders.EXPECT_WITHIN, deadlineHeader);
        }
        if (enforcementHeader != null) {
            inbound.put(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, enforcementHeader);
        }
        try (CallContextScope ignored = parse(Optional.of(Duration.ofSeconds(10)), inbound, localEnforcement)
                .attach()) {
            assertThat(Deadlines.getEnforcement()).contains(expectedEnforcement);

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(outbound.get(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED))
                    .isEqualTo(expectedHeader);
        }
    }

    @Test
    void encode_to_request_respects_client_enforcement_strategy() {
        Map<String, String> inboundRequest = new HashMap<>();
        inboundRequest.put(
                DeadlinesHttpHeaders.EXPECT_WITHIN,
                Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), inboundRequest, Enforcement.DEFER).attach()) {
            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();

            Deadlines.encodeToRequest(
                    Duration.ofSeconds(2), outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);
            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED)))
                    .isEmpty();

            Deadlines.encodeToRequest(
                    Duration.ofSeconds(2), outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.ENFORCE);
            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED)))
                    .contains("true");

            Deadlines.encodeToRequest(
                    Duration.ofSeconds(2), outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.DISABLE);
            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED)))
                    .contains("false");
        }
    }

    @Test
    void encode_to_request_resolves_client_enforcement_with_existing() {
        Map<String, String> inboundRequest = new HashMap<>();
        inboundRequest.put(
                DeadlinesHttpHeaders.EXPECT_WITHIN,
                Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), inboundRequest, Enforcement.DISABLE).attach()) {
            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(2), outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.ENFORCE);
            // existing DISABLE strategy takes precedence over requested ENFORCE strategy
            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED)))
                    .contains("false");
        }
    }

    @Test
    void encode_to_request_throws_on_expired_deadline_when_client_requests_enforcement() {
        Map<String, String> inboundRequest = new HashMap<>();
        inboundRequest.put(
                DeadlinesHttpHeaders.EXPECT_WITHIN,
                Deadlines.durationToHeaderValue(Duration.ofSeconds(0).toNanos()));
        try (CallContextScope ignored =
                parse(Optional.empty(), inboundRequest, Enforcement.DEFER).attach()) {
            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            assertThatExceptionOfType(DeadlineExpiredException.External.class)
                    .isThrownBy(() -> Deadlines.encodeToRequest(
                            Duration.ofSeconds(2), outboundRequest, DummyRequestEncoder.INSTANCE, Enforcement.ENFORCE));
        }
    }

    @ParameterizedTest
    @CsvSource({
        "0.05, SUB_100MS",
        "0.5, SUB_1S",
        "5, SUB_10S",
        "50, SUB_100S",
        "500, ABOVE_100S",
    })
    public void expired_budget_meter_records_correct_bucket(String deadlineSeconds, Expired_Budget expectedBucket) {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        Map<String, String> request = new HashMap<>();
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, deadlineSeconds);
        try (CallContextScope ignored =
                parse(Optional.empty(), request, Enforcement.DISABLE).attach()) {
            clock.advance(Duration.ofNanos(Long.MAX_VALUE));

            @SuppressWarnings("for-rollout:deprecation")
            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter expectedMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .budget(expectedBucket)
                    .build();
            long originalCount = expectedMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(
                    Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE, Enforcement.DEFER);

            assertThat(expectedMeter.getCount()).isEqualTo(originalCount + 1);
        }
    }

    @Nested
    class CheckDeadlineTests {
        private final TestClock clock = new TestClock();

        @BeforeEach
        void before() {
            Deadlines.setClock(clock);
        }

        @Test
        void no_op_without_a_deadline() {
            assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE)).doesNotThrowAnyException();
        }

        @Test
        void no_op_before_the_deadline_expires() {
            try (CallContextScope ignored =
                    internal(Duration.ofSeconds(5), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofNanos(4_999_999_999L));

                assertThatCode(() -> Deadlines.checkDeadline(Enforcement.DEFER)).doesNotThrowAnyException();
            }
        }

        @ParameterizedTest
        @CsvSource({"5000000000", "6000000000"})
        void throws_once_an_enforced_deadline_has_expired(long elapsedNanos) {
            try (CallContextScope ignored =
                    internal(Duration.ofSeconds(5), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofNanos(elapsedNanos));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.Internal.class);
            }
        }

        @Test
        void reports_an_externally_imposed_deadline_as_external() {
            try (CallContextScope ignored = parse(
                            Optional.empty(), Map.of(DeadlinesHttpHeaders.EXPECT_WITHIN, "5"), Enforcement.ENFORCE)
                    .attach()) {
                clock.advance(Duration.ofSeconds(6));

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .isInstanceOf(DeadlineExpiredException.External.class);
            }
        }

        @Test
        void resolves_enforcement_against_the_caller() {
            try (CallContextScope ignored =
                    internal(Duration.ofSeconds(5), Enforcement.DEFER).attach()) {
                clock.advance(Duration.ofSeconds(6));

                assertThatCode(() -> Deadlines.checkDeadline(Enforcement.DEFER))
                        .as("neither side asked for enforcement")
                        .doesNotThrowAnyException();
                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .as("the caller opts in, so a deferred trace deadline is enforced")
                        .isInstanceOf(DeadlineExpiredException.class);
            }
        }

        @Test
        void caller_cannot_enforce_a_disabled_trace_deadline() {
            try (CallContextScope ignored =
                    internal(Duration.ofSeconds(5), Enforcement.DISABLE).attach()) {
                clock.advance(Duration.ofSeconds(6));

                assertThatCode(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .doesNotThrowAnyException();
            }
        }

        @Test
        void records_the_expiration_it_throws_for() {
            try (CallContextScope ignored =
                    internal(Duration.ofSeconds(5), Enforcement.ENFORCE).attach()) {
                clock.advance(Duration.ofSeconds(6));
                long before = expiredMeter(Expired_Cause.INTERNAL, Expired_Intent.THROW)
                        .getCount();

                assertThatThrownBy(() -> Deadlines.checkDeadline(Enforcement.ENFORCE))
                        .isInstanceOf(DeadlineExpiredException.class);

                assertThat(expiredMeter(Expired_Cause.INTERNAL, Expired_Intent.THROW)
                                .getCount())
                        .isEqualTo(before + 1);
            }
        }
    }

    // A context with the deadline of an inbound request.
    private static CallContext parse(
            Optional<Duration> internalDeadline, Map<String, String> request, Enforcement enforcement) {
        return Deadlines.withRequestDeadline(
                CallContext.empty(), internalDeadline, request, DummyRequestDecoder.INSTANCE, enforcement);
    }

    // A context with an internal deadline, as configured for an endpoint.
    private static CallContext internal(Duration deadline, Enforcement enforcement) {
        return parse(Optional.of(deadline), Map.of(), enforcement);
    }

    @SuppressWarnings("for-rollout:deprecation")
    private static Meter expiredMeter(Expired_Cause cause, Expired_Intent intent) {
        return DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton())
                .expired()
                .cause(cause)
                .intent(intent)
                .budget(Expired_Budget.SUB_10S)
                .build();
    }

    private enum DummyRequestEncoder implements RequestEncodingAdapter<Map<String, String>> {
        INSTANCE;

        @Override
        public void setHeader(Map<String, String> headers, String headerName, String headerValue) {
            headers.put(headerName, headerValue);
        }
    }

    private enum DummyRequestDecoder implements RequestDecodingAdapter<Map<String, String>> {
        INSTANCE;

        @Override
        public Optional<String> getFirstHeader(Map<String, String> _headers, String _headerName) {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public @Nullable String maybeFirstHeader(Map<String, String> headers, String headerName) {
            return headers.get(headerName);
        }
    }
}
