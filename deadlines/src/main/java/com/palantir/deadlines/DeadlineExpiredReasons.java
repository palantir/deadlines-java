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

import java.util.Optional;

public final class DeadlineExpiredReasons {

    private DeadlineExpiredReasons() {}

    public static <T> void encodeToResponse(
            DeadlineExpiredException exception, T response, ResponseEncodingAdapter<T> adapter) {
        if (exception instanceof DeadlineExpiredException.External) {
            adapter.setHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "external");
        } else if (exception instanceof DeadlineExpiredException.Internal) {
            adapter.setHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "internal");
        }
    }

    public static <T> Optional<DeadlineExpiredException> parseFromResponse(
            T response, ResponseDecodingAdapter<T> adapter) {
        Optional<String> reason = adapter.getFirstHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON);
        return reason.flatMap(s -> switch (s) {
            case "external" -> Optional.of(DeadlineExpiredException.external());
            case "internal" -> Optional.of(DeadlineExpiredException.internal());
            default -> Optional.empty();
        });
    }

    public static int getHttpStatusCode(DeadlineExpiredException exception) {
        if (exception instanceof DeadlineExpiredException.External) {
            // external deadline expiration is considered a client error
            return 400;
        } else if (exception instanceof DeadlineExpiredException.Internal) {
            // internal deadline expiration is considered a server error
            return 500;
        } else {
            // anything else is considered a server error
            return 500;
        }
    }

    public interface ResponseEncodingAdapter<RESPONSE> {
        void setHeader(RESPONSE response, String headerName, String headerValue);
    }

    public interface ResponseDecodingAdapter<RESPONSE> {
        Optional<String> getFirstHeader(RESPONSE response, String headerName);
    }
}
