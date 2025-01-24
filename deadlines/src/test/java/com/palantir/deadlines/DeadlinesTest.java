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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.codahale.metrics.Meter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class DeadlinesTest {

    @Test
    public void test_duration_to_header_value() {
        long duration = Duration.ofMillis(1523).toNanos();
        String headerValue = Deadlines.durationToHeaderValue(duration);
        assertThat(headerValue).isEqualTo("1.523");
    }

    @Test
    public void test_header_value_to_duration() {
        String headerValue = "1.523";
        long duration = Deadlines.headerValueToDuration(headerValue);
        assertThat(duration).isEqualTo(Duration.ofMillis(1523).toNanos());
    }

    @Test
    public void can_encode_to_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration deadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(deadline, request, new DummyRequestEncoder());

            assertThat(request.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(s -> {
                        String expected = Deadlines.durationToHeaderValue(deadline.toNanos());
                        assertThat(s).isEqualTo(expected);
                    });
        }
    }

    @Test
    public void can_parse_from_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            request.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, new DummyRequestDecoder());

            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isLessThanOrEqualTo(providedDeadline));
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_internal_state() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest inboundRequest = new DummyRequest();
            inboundRequest.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN,
                    Deadlines.durationToHeaderValue(Duration.ofSeconds(1).toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), inboundRequest, new DummyRequestDecoder());

            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            DummyRequest outboundRequest = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(2);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, new DummyRequestEncoder());

            assertThat(outboundRequest.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(h -> {
                        long parsed = Deadlines.headerValueToDuration(h);
                        assertThat(parsed)
                                .isLessThanOrEqualTo(stateDeadline.get().toNanos());
                    });
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_argument() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest inboundRequest = new DummyRequest();
            inboundRequest.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN,
                    Deadlines.durationToHeaderValue(Duration.ofSeconds(2).toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), inboundRequest, new DummyRequestDecoder());

            Optional<Duration> stateDeadline = Deadlines.getRemainingDeadline();
            assertThat(stateDeadline).isPresent();

            DummyRequest outboundRequest = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, new DummyRequestEncoder());

            assertThat(outboundRequest.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(h -> {
                        long parsed = Deadlines.headerValueToDuration(h);
                        assertThat(parsed).isLessThanOrEqualTo(providedDeadline.toNanos());
                    });
        }
    }

    @Test
    public void parse_from_request_noop_when_no_header_present() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Deadlines.parseFromRequest(Optional.empty(), request, new DummyRequestDecoder());
            assertThat(Deadlines.getRemainingDeadline()).isEmpty();
        }
    }

    @Test
    public void parse_from_request_noop_when_no_trace() {
        DummyRequest request = new DummyRequest();
        Duration providedDeadline = Duration.ofSeconds(1);
        request.setHeader(
                DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
        Deadlines.parseFromRequest(Optional.empty(), request, new DummyRequestDecoder());
        assertThat(Deadlines.getRemainingDeadline()).isEmpty();
    }

    @Test
    public void test_expiration_get_remaining_deadline() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofMillis(1);
            request.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, new DummyRequestDecoder());

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
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofMillis(1);
            request.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.empty(), request, new DummyRequestDecoder());

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

            DummyRequest outbound = new DummyRequest();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound, new DummyRequestEncoder());

            assertThat(externalMeter.getCount()).isGreaterThan(originalExternalValue);
            assertThat(internalMeter.getCount()).isEqualTo(originalInternalValue);
        }
    }

    @Test
    public void test_encode_to_request_expiration_internal_deadline() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofMillis(100);
            request.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline.toNanos()));
            Deadlines.parseFromRequest(Optional.of(Duration.ofMillis(1)), request, new DummyRequestDecoder());

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

            DummyRequest outbound = new DummyRequest();
            Deadlines.encodeToRequest(Duration.ofSeconds(10), outbound, new DummyRequestEncoder());

            assertThat(internalMeter.getCount()).isGreaterThan(originalInternalValue);
            assertThat(externalMeter.getCount()).isEqualTo(originalExternalValue);
        }
    }

    private static final class DummyRequest {
        private final Multimap<String, String> headers = ArrayListMultimap.create();

        void setHeader(String name, String value) {
            headers.put(name, value);
        }

        Optional<String> getFirstHeader(String name) {
            return headers.get(name).stream().findFirst();
        }
    }

    private static final class DummyRequestEncoder implements RequestEncodingAdapter<DummyRequest> {
        @Override
        public void setHeader(DummyRequest dummyRequest, String headerName, String headerValue) {
            dummyRequest.setHeader(headerName, headerValue);
        }
    }

    private static final class DummyRequestDecoder implements RequestDecodingAdapter<DummyRequest> {
        @Override
        public Optional<String> getFirstHeader(DummyRequest dummyRequest, String headerName) {
            return dummyRequest.getFirstHeader(headerName);
        }
    }
}
