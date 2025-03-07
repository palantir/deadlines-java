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

package com.palantir.deadlines;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.codahale.metrics.Meter;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.tracing.CloseableSpan;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import net.jqwik.api.ForAll;
import net.jqwik.api.GenerationMode;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.LowerChars;
import net.jqwik.api.constraints.NumericChars;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.constraints.Whitespace;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class DeadlinesTest {

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
                "", // Empty string
                " ", // String with only a space
                ",", // String with only a comma
                ".", // String with only a decimal
                "foo", // alpha only
                "1,234", // comma separator
                "1e", // exponent
                "1.2.3", // Double decimal
                "1 2", // numbers with space
                "1.2 3", // decimal with space
                "12x", //
                ".", // Decimal only
                "-1.-2-3", //
                "-123", // Negative integer
                "-123.456", // Negative decimal
                "1.234e5", // Scientific notation
                "123.456e2", // Scientific notation
                "-123.456e-2", // Negative number in scientific notation
                "1.23E4", // Uppercase scientific notation
                "1,234.56", // Comma as a thousand separator (if locale supports it)
                "abc", // Non-numeric characters
                "123abc", // Mixed numeric and non-numeric characters
                "123..456", // Multiple decimal points
                "-123.-456", // Misplaced negative sign
                "123e", // Incomplete scientific notation
                "e123", // Scientific notation without base
                "123e++10", // Invalid exponent format
                "123,456.78", // Comma as a thousand separator without proper locale
                "123.456.789", // Multiple decimal points
                "NaN", // Special floating-point value
                "Infinity", // Special floating-point value
                "-Infinity", // Negative special floating-point value
                "0x123", // Hexadecimal notation
            })
    public void test_invalid_header_value_to_duration(String headerValue) {
        assertThat(Deadlines.tryParseSecondsToNanoseconds(headerValue)).isNull();
    }

    @Property(tries = 100_000, generation = GenerationMode.AUTO)
    void check_tryParseSecondsToNanoseconds_successfully_parses_numeric_values(
            @ForAll @NumericChars @StringLength(min = 1, max = 100) String integer,
            @ForAll @NumericChars @StringLength(min = 1, max = 100) String decimal) {
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
    }

    @Property(tries = 100_000, generation = GenerationMode.AUTO)
    void check_tryParseSecondsToNanoseconds_handles_inputs(
            @ForAll @AlphaChars @NumericChars @Whitespace @LowerChars @StringLength(min = 0, max = 100) String input) {
        assertThatCode(() -> Deadlines.tryParseSecondsToNanoseconds(input)).doesNotThrowAnyException();
    }

    @Test
    public void can_encode_to_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration deadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(deadline, request, DummyRequestEncoder.INSTANCE);

            assertThat(Optional.ofNullable(request.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                    .hasValueSatisfying(s -> {
                        String expected = Deadlines.durationToHeaderValue(deadline.toNanos());
                        assertThat(s).isEqualTo(expected);
                    });
        }
    }

    @Test
    public void can_parse_from_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isLessThanOrEqualTo(providedDeadline));
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_internal_state() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> inboundRequest = new HashMap<>();
            long originalDeadline = Duration.ofSeconds(1).toNanos();
            inboundRequest.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(originalDeadline));
            Deadlines.parseFromRequest(Optional.empty(), inboundRequest, DummyRequestDecoder.INSTANCE);

            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(2);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, DummyRequestEncoder.INSTANCE);

            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                    .hasValueSatisfying(h -> {
                        Long parsed = Deadlines.tryParseSecondsToNanoseconds(h);
                        assertThat(parsed).isNotNull().isLessThanOrEqualTo(originalDeadline);
                    });
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_argument() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> inboundRequest = new HashMap<>();
            inboundRequest.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN,
                    Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), inboundRequest, DummyRequestDecoder.INSTANCE);

            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Map<String, String> outboundRequest = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, DummyRequestEncoder.INSTANCE);

            assertThat(Optional.ofNullable(outboundRequest.get(DeadlinesHttpHeaders.EXPECT_WITHIN)))
                    .hasValueSatisfying(h -> {
                        Long parsed = Deadlines.tryParseSecondsToNanoseconds(h);
                        assertThat(parsed).isLessThanOrEqualTo(providedDeadline.toNanos());
                    });
        }
    }

    @Test
    public void encode_to_request_noop_when_propagation_disabled() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> inboundRequest = new HashMap<>();
            inboundRequest.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN,
                    Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), inboundRequest, DummyRequestDecoder.INSTANCE);

            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            Deadlines.disableFurtherDeadlinePropagation();

            Map<String, String> outboundRequest = new HashMap<>();
            Duration providedDeadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, DummyRequestEncoder.INSTANCE);

            // even with a provided deadline lower than the one from state, disabling propagation should prevent
            // further encoding of headers
            assertThat(outboundRequest).isEmpty();

            // getRemainingDeadline should always return empty now
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
        }
    }

    @Test
    public void parse_from_request_noop_when_no_header_present() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
        }
    }

    @Test
    public void parse_from_request_noop_when_no_trace() {
        Map<String, String> request = new HashMap<>();
        Duration providedDeadline = Duration.ofSeconds(1);
        request.put(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);
        assertThat(Deadlines.getRemainingDeadline()).isEmpty();
    }

    @Test
    public void test_expiration_get_remaining_deadline() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            clock.elapsed += 2_000_000;

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));
        }
    }

    @Test
    public void test_encode_to_request_expiration_external_deadline() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            clock.elapsed += 2_000_000;

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));

            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            Meter internalMeter = metrics.expired()
                    .cause(Expired_Cause.INTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            long originalExternalValue = externalMeter.getCount();
            long originalInternalValue = internalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE);

            assertThat(externalMeter.getCount()).isGreaterThan(originalExternalValue);
            assertThat(internalMeter.getCount()).isEqualTo(originalInternalValue);
        }
    }

    @Test
    public void test_encode_to_request_expiration_internal_deadline() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(100);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.of(Duration.ofMillis(1)), request, DummyRequestDecoder.INSTANCE);

            clock.elapsed += 2_000_000;
            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));

            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            Meter internalMeter = metrics.expired()
                    .cause(Expired_Cause.INTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            long originalExternalValue = externalMeter.getCount();
            long originalInternalValue = internalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE);

            assertThat(internalMeter.getCount()).isGreaterThan(originalInternalValue);
            assertThat(externalMeter.getCount()).isEqualTo(originalExternalValue);
        }
    }

    @Test
    public void disabled_propagation_reports_metrics_on_expiration() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            clock.elapsed += 2_000_000;

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));

            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeterWillPropagate = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            Meter externalMeterWontPropagate = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.IGNORE)
                    .build();
            long originalWillPropagateValue = externalMeterWillPropagate.getCount();
            long originalWontPropagateValue = externalMeterWontPropagate.getCount();

            // first request is allowed to propagate the deadline, make sure the correct meter is marked
            Map<String, String> outbound1 = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound1, DummyRequestEncoder.INSTANCE);
            assertThat(externalMeterWillPropagate.getCount()).isGreaterThan(originalWillPropagateValue);
            assertThat(externalMeterWontPropagate.getCount()).isEqualTo(originalWontPropagateValue);

            originalWillPropagateValue = externalMeterWillPropagate.getCount();
            originalWontPropagateValue = externalMeterWontPropagate.getCount();

            // and now disable propagation
            Deadlines.disableFurtherDeadlinePropagation();

            // second request is not allowed to propagate the deadline, make sure the correct meter is marked
            Map<String, String> outbound2 = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound2, DummyRequestEncoder.INSTANCE);
            assertThat(externalMeterWontPropagate.getCount()).isGreaterThan(originalWontPropagateValue);
            assertThat(externalMeterWillPropagate.getCount()).isEqualTo(originalWillPropagateValue);
        }
    }

    @Test
    public void multihop_expired_received_deadline_marks_propagate_already_expired_meter() {
        TestClock clock = new TestClock();
        Deadlines.setClock(clock);

        DetachedSpan server1Span = DetachedSpan.start("server1");
        DetachedSpan server2Span = DetachedSpan.start("server2");

        try (CloseableSpan ignored = server1Span.attach()) {
            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter expiredMeterPropagateIntent = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE)
                    .build();
            Meter expiredMeterPropagateAlreadyExpiredIntent = metrics.expired()
                    .cause(Expired_Cause.EXTERNAL)
                    .intent(Expired_Intent.PROPAGATE_ALREADY_EXPIRED)
                    .build();

            // the first hop receives a valid, non-zero deadline on the wire
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);
            // nothing yet...
            assertThat(expiredMeterPropagateIntent.getCount()).isZero();
            assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount()).isZero();

            // force expiration within the first hop
            clock.elapsed += 2_000_000;

            long expiredMeterPropagateIntentValue = expiredMeterPropagateIntent.getCount();
            long expiredMeterPropagateAlreadyExpiredIntentValue = expiredMeterPropagateAlreadyExpiredIntent.getCount();

            Map<String, String> outbound1 = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound1, DummyRequestEncoder.INSTANCE);

            // this hop marks the meter with the "propagate" intent, as the expiration happened here
            assertThat(outbound1.get(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .isNotNull()
                    .isEqualTo("0");
            assertThat(expiredMeterPropagateIntent.getCount()).isGreaterThan(expiredMeterPropagateIntentValue);
            assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount()).isZero();

            // next hop parses a zero deadline
            try (CloseableSpan ignored2 = server2Span.attach()) {
                expiredMeterPropagateIntentValue = expiredMeterPropagateIntent.getCount();
                Deadlines.parseFromRequest(Optional.empty(), outbound1, DummyRequestDecoder.INSTANCE);
                // sending another request when the deadline has already expired should
                // mark the meter with the "propagate-already-expired" intent
                expiredMeterPropagateAlreadyExpiredIntentValue = expiredMeterPropagateAlreadyExpiredIntent.getCount();
                Map<String, String> outbound2 = new HashMap<>();
                Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound2, DummyRequestEncoder.INSTANCE);
                assertThat(expiredMeterPropagateAlreadyExpiredIntent.getCount())
                        .isGreaterThan(expiredMeterPropagateAlreadyExpiredIntentValue);
                // meter with the "propagate" intent is unchanged
                assertThat(expiredMeterPropagateIntent.getCount()).isEqualTo(expiredMeterPropagateIntentValue);
            }
        }
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

    private static final class TestClock implements Deadlines.Clock {
        private long elapsed = 0L;

        @Override
        public long nanoTime() {
            return elapsed;
        }
    }
}
