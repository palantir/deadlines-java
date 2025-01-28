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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.api.DeadlineExpiredException;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class DeadlinesTest {

    @Test
    public void test_duration_to_header_value() {
        Duration duration = Duration.ofMillis(1523);
        String headerValue = Deadlines.durationToHeaderValue(duration);
        assertThat(headerValue).isEqualTo("1.523");
    }

    @Test
    public void test_header_value_to_duration() {
        String headerValue = "1.523";
        Duration duration = Deadlines.headerValueToDuration(headerValue);
        assertThat(duration).isEqualTo(Duration.ofMillis(1523));
    }

    @Test
    public void can_encode_to_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration deadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(deadline, request, new DummyRequestEncoder());

            assertThat(request.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(s -> {
                        String expected = Deadlines.durationToHeaderValue(deadline);
                        assertThat(s).isEqualTo(expected);
                    });
        }
    }

    @Test
    public void can_parse_from_request() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
            Optional<Duration> deadline = Deadlines.parseFromRequest(request, new DummyRequestDecoder());

            assertThat(deadline).hasValueSatisfying(d -> assertThat(d).isEqualTo(providedDeadline));
        }
    }

    @Test
    public void parse_from_request_stores_internal_state() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
            Optional<Duration> deadline = Deadlines.parseFromRequest(request, new DummyRequestDecoder());
            Optional<Duration> remaining = Deadlines.getRemainingDeadline();
            assertThat(deadline).isPresent();
            assertThat(remaining).hasValueSatisfying(d -> assertThat(d).isLessThanOrEqualTo(deadline.get()));
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_internal_state() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest inboundRequest = new DummyRequest();
            inboundRequest.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(Duration.ofSeconds(1)));
            Optional<Duration> stateDeadline = Deadlines.parseFromRequest(inboundRequest, new DummyRequestDecoder());

            assertThat(stateDeadline).isPresent();

            DummyRequest outboundRequest = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(2);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, new DummyRequestEncoder());

            assertThat(outboundRequest.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(h -> {
                        Duration parsed = Deadlines.headerValueToDuration(h);
                        assertThat(parsed).isLessThanOrEqualTo(stateDeadline.get());
                    });
        }
    }

    @Test
    public void encode_to_request_uses_smaller_deadline_from_argument() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest inboundRequest = new DummyRequest();
            inboundRequest.setHeader(
                    DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(Duration.ofSeconds(2)));
            Optional<Duration> stateDeadline = Deadlines.parseFromRequest(inboundRequest, new DummyRequestDecoder());

            assertThat(stateDeadline).isPresent();

            DummyRequest outboundRequest = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(providedDeadline, outboundRequest, new DummyRequestEncoder());

            assertThat(outboundRequest.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .hasValueSatisfying(h -> {
                        Duration parsed = Deadlines.headerValueToDuration(h);
                        assertThat(parsed).isLessThanOrEqualTo(providedDeadline);
                    });
        }
    }

    @Test
    public void parse_from_request_noop_when_no_header_present() {
        DummyRequest request = new DummyRequest();
        Optional<Duration> result = Deadlines.parseFromRequest(request, new DummyRequestDecoder());
        assertThat(result).isEmpty();
    }

    @Test
    public void parse_from_request_noop_when_no_trace() {
        DummyRequest request = new DummyRequest();
        Duration providedDeadline = Duration.ofSeconds(1);
        request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
        Optional<Duration> deadline = Deadlines.parseFromRequest(request, new DummyRequestDecoder());

        // the parse method still returns a duration equal to the provided deadline...
        assertThat(deadline).hasValueSatisfying(d -> assertThat(d).isEqualTo(providedDeadline));
        // but state is not retained
        assertThat(Deadlines.getRemainingDeadline()).isEmpty();
    }

    @Test
    public void test_expiration_get_remaining_deadline() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofMillis(1);
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
            Deadlines.parseFromRequest(request, new DummyRequestDecoder());

            // pollInSameThread is necessary for the polling function's call to getRemainingDeadline
            // to read state from the TraceLocal
            Awaitility.pollInSameThread();
            Awaitility.waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Optional<Duration> remaining = Deadlines.getRemainingDeadline();
                assertThat(remaining).isPresent().hasValue(Duration.ZERO);
            });
        }
    }

    @Test
    public void encode_to_request_throws_when_deadline_from_internal_state_expired() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofMillis(1);
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
            Deadlines.parseFromRequest(request, new DummyRequestDecoder());

            Awaitility.pollInSameThread();
            Awaitility.waitAtMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThatThrownBy(() -> {
                        DummyRequest outbound = new DummyRequest();
                        Deadlines.encodeToRequest(Duration.ofSeconds(1), outbound, new DummyRequestEncoder());
                    })
                    .isInstanceOf(DeadlineExpiredException.class));
        }
    }

    @Test
    public void encode_to_request_throws_when_deadline_from_args_expired() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ofSeconds(1);
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));
            Deadlines.parseFromRequest(request, new DummyRequestDecoder());

            assertThatThrownBy(() -> {
                        DummyRequest outbound = new DummyRequest();
                        Deadlines.encodeToRequest(Duration.ZERO, outbound, new DummyRequestEncoder());
                    })
                    .isInstanceOf(DeadlineExpiredException.class);
        }
    }

    @Test
    public void parse_from_request_throws_when_deadline_expired() {
        try (CloseableTracer tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration providedDeadline = Duration.ZERO;
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, Deadlines.durationToHeaderValue(providedDeadline));

            assertThatThrownBy(() -> Deadlines.parseFromRequest(request, new DummyRequestDecoder()))
                    .isInstanceOf(DeadlineExpiredException.class);
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
