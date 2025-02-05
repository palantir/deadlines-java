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

import com.codahale.metrics.Meter;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.awaitility.Awaitility;
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
        "1, 1000000000",
        "' 2. ', 2000000000",
        "3.0, 3000000000",
        "3.1, 3100000000",
        "1234567890.12345, 1234567890123450112",
        "1.523, 1523000000",
        "'   1.523  ', 1523000000",
    })
    public void test_header_value_to_duration(String input, long expectedNanos) {
        assertThat(Deadlines.tryParseSecondsToNanoseconds(input))
                .isNotNull()
                .isEqualTo(expectedNanos)
                .isEqualTo(Math.round(Double.parseDouble(input) * 1_000_000_000L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", ",", ".", "d", "1,234", "-1.234", "1.234e5"})
    public void test_invalid_header_value_to_duration(String headerValue) {
        assertThat(Deadlines.tryParseSecondsToNanoseconds(headerValue)).isNull();
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
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            // pollInSameThread is necessary for the polling function's call to getRemainingDeadline
            // to read state from the TraceLocal
            Awaitility.pollInSameThread();
            Awaitility.waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Optional<Duration> remaining = Deadlines.getRemainingDeadline();
                assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));
            });
        }
    }

    @Test
    public void test_encode_to_request_expiration_external_deadline() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(1);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, DummyRequestDecoder.INSTANCE);

            // wait until we know the deadline has expired
            Awaitility.pollInSameThread();
            Awaitility.waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Optional<Duration> remaining = Deadlines.getRemainingDeadline();
                assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));
            });

            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired(Expired_Cause.EXTERNAL);
            Meter internalMeter = metrics.expired(Expired_Cause.INTERNAL);
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
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            Map<String, String> request = new HashMap<>();
            Duration providedDeadline = Duration.ofMillis(100);
            request.put(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.of(Duration.ofMillis(1)), request, DummyRequestDecoder.INSTANCE);

            // wait until we know the deadline has expired
            Awaitility.pollInSameThread();
            Awaitility.waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Optional<Duration> remaining = Deadlines.getRemainingDeadline();
                assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isEqualTo(Duration.ZERO));
            });

            DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
            Meter externalMeter = metrics.expired(Expired_Cause.EXTERNAL);
            Meter internalMeter = metrics.expired(Expired_Cause.INTERNAL);
            long originalExternalValue = externalMeter.getCount();
            long originalInternalValue = internalMeter.getCount();

            Map<String, String> outbound = new HashMap<>();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound, DummyRequestEncoder.INSTANCE);

            assertThat(internalMeter.getCount()).isGreaterThan(originalInternalValue);
            assertThat(externalMeter.getCount()).isEqualTo(originalExternalValue);
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
        public Optional<String> getFirstHeader(Map<String, String> headers, String headerName) {
            return Optional.ofNullable(headers.get(headerName));
        }
    }
}
