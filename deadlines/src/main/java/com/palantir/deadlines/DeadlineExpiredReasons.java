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

import javax.annotation.Nullable;

public final class DeadlineExpiredReasons {

    private DeadlineExpiredReasons() {}

    public static <T> void encodeToResponse(
            DeadlineExpiredException exception, T response, ResponseEncodingAdapter<T> adapter) {
        if (exception instanceof DeadlineExpiredException.External) {
            adapter.setHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "external");
            // external deadline expiration is considered a client error
            adapter.setStatus(response, 400);
        } else if (exception instanceof DeadlineExpiredException.Internal) {
            adapter.setHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON, "internal");
            // server deadline expiration is considered a server error
            adapter.setStatus(response, 500);
        }
    }

    @SuppressWarnings("for-rollout:AnnotationPosition")
    public static <T> @Nullable DeadlineExpiredException maybeParseFromResponse(
            T response, ResponseDecodingAdapter<T> adapter) {
        String reason = adapter.maybeFirstHeader(response, DeadlinesHttpHeaders.DEADLINE_EXPIRED_REASON);
        int status = adapter.getStatus(response);
        if (reason == null) {
            return null;
        }
        if (reason.equalsIgnoreCase("external") && status == 400) {
            return DeadlineExpiredException.external();
        } else if (reason.equalsIgnoreCase("internal") && status == 500) {
            return DeadlineExpiredException.internal();
        } else {
            return null;
        }
    }

    public interface ResponseEncodingAdapter<RESPONSE> {
        void setHeader(RESPONSE response, String headerName, String headerValue);

        void setStatus(RESPONSE response, int status);
    }

    public interface ResponseDecodingAdapter<RESPONSE> {
        @Nullable
        String maybeFirstHeader(RESPONSE response, String headerName);

        int getStatus(RESPONSE response);
    }
}
