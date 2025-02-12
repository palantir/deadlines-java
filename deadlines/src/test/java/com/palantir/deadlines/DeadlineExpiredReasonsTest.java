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
import static org.assertj.core.api.Assertions.entry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeadlineExpiredReasonsTest {

    @Test
    public void encodes_external_to_response() {
        TestResponse response = new TestResponse();
        DeadlineExpiredReasons.encodeToResponse(DeadlineExpiredException.external(), response, Encoder.INSTANCE);
        assertThat(response.status).isEqualTo(400);
        assertThat(response.headers).contains(entry(DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "external"));
    }

    @Test
    public void encodes_internal_to_response() {
        TestResponse response = new TestResponse();
        DeadlineExpiredReasons.encodeToResponse(DeadlineExpiredException.internal(), response, Encoder.INSTANCE);
        assertThat(response.status).isEqualTo(500);
        assertThat(response.headers).contains(entry(DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "internal"));
    }

    @Test
    public void decodes_external_from_response() {
        TestResponse response = new TestResponse();
        response.status = 400;
        response.headers = Map.of(DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "external");
        Optional<DeadlineExpiredException> result =
                DeadlineExpiredReasons.parseFromResponse(response, Decoder.INSTANCE);
        assertThat(result).hasValueSatisfying(e -> assertThat(e).isInstanceOf(DeadlineExpiredException.External.class));
    }

    @Test
    public void decodes_internal_from_response() {
        TestResponse response = new TestResponse();
        response.status = 500;
        response.headers = Map.of(DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "internal");
        Optional<DeadlineExpiredException> result =
                DeadlineExpiredReasons.parseFromResponse(response, Decoder.INSTANCE);
        assertThat(result).hasValueSatisfying(e -> assertThat(e).isInstanceOf(DeadlineExpiredException.Internal.class));
    }

    @Test
    public void decodes_noop_when_missing_header() {
        TestResponse response = new TestResponse();
        response.status = 500;
        Optional<DeadlineExpiredException> result =
                DeadlineExpiredReasons.parseFromResponse(response, Decoder.INSTANCE);
        assertThat(result).isEmpty();
    }

    @Test
    public void decodes_noop_when_bad_header_value() {
        TestResponse response = new TestResponse();
        response.status = 500;
        response.headers = Map.of(DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "asdf");
        Optional<DeadlineExpiredException> result =
                DeadlineExpiredReasons.parseFromResponse(response, Decoder.INSTANCE);
        assertThat(result).isEmpty();
    }

    private static final class TestResponse {
        private int status;
        private Map<String, String> headers = new HashMap<>();
    }

    private enum Encoder implements DeadlineExpiredReasons.ResponseEncodingAdapter<TestResponse> {
        INSTANCE;

        @Override
        public void setHeader(TestResponse testResponse, String headerName, String headerValue) {
            testResponse.headers.put(headerName, headerValue);
        }

        @Override
        public void setStatus(TestResponse testResponse, int status) {
            testResponse.status = status;
        }
    }

    private enum Decoder implements DeadlineExpiredReasons.ResponseDecodingAdapter<TestResponse> {
        INSTANCE;

        @Override
        public Optional<String> getFirstHeader(TestResponse testResponse, String headerName) {
            return Optional.ofNullable(testResponse.headers.get(headerName));
        }
    }
}
