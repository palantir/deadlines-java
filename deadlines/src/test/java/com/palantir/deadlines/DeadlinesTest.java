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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeadlinesTest {
    @Test
    public void can_store_and_retrieve_deadline() {
        try (CloseableTracer _tracer = CloseableTracer.startSpan("test")) {
            Deadlines.putDeadline(Duration.ofSeconds(1));
            Optional<Duration> remainingDeadline = Deadlines.getRemainingDeadline();
            assertThat(remainingDeadline).hasValueSatisfying(value -> {
                assertThat(value).isPositive();
            });
        }
    }

    @Test
    public void noop_when_outside_of_trace() {
        Deadlines.putDeadline(Duration.ofSeconds(1));
        Optional<Duration> remainingDeadline = Deadlines.getRemainingDeadline();
        assertThat(remainingDeadline).isEmpty();
    }

    @Test
    public void can_encode_to_http_request() {
        try (CloseableTracer _tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            Duration deadline = Duration.ofSeconds(1);
            Deadlines.encodeToRequest(deadline, request, new DummyRequestEncoder());

            assertThat(request.getFirstHeader(DeadlinesHttpHeaders.EXPECT_WITHIN))
                    .isPresent()
                    .hasValue("1.00000");
        }
    }

    @Test
    public void can_decode_from_http_request() {
        try (CloseableTracer _tracer = CloseableTracer.startSpan("test")) {
            DummyRequest request = new DummyRequest();
            request.setHeader(DeadlinesHttpHeaders.EXPECT_WITHIN, "1.00000");
            Optional<Duration> deadline = Deadlines.parseFromRequest(request, new DummyRequestDecoder());

            assertThat(deadline).isPresent().hasValue(Duration.ofSeconds(1));
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