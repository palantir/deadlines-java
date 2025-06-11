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

/**
 * Defines HTTP header names used by deadlines.
 *
 * When sending or parsing deadline metadata on HTTP requests, consumers typically do not need
 * to interact with the constants in this class directly. Instead, use {@link Deadlines#encodeToRequest}
 * and {@link Deadlines#parseFromRequest} with a supplied {@link Deadlines.RequestEncodingAdapter} to encode
 * or decode header values from an HTTP request implementation type.
 */
@SuppressWarnings("for-rollout:InterfaceWithOnlyStatics")
public interface DeadlinesHttpHeaders {
    /**
     * Expect-Within is a client-provided deadline value, specified in decimal number of seconds.
     *
     * It indicates to a server that client expects a response within the given time, and may abort the request
     * after that period of time.
     */
    String EXPECT_WITHIN = "Expect-Within";

    /**
     * Expect-Within-Enforced is a boolean that will be set to "true" when deadline enforcement is requested.
     */
    String EXPECT_WITHIN_ENFORCED = "Expect-Within-Enforced";

    /**
     * Deadline-Expired-Reason is a string indicating the reason for a deadline expiration (e.g. external vs internal).
     *
     * See also {@link DeadlineExpiredReasons}.
     */
    String DEADLINE_EXPIRED_REASON = "Deadline-Expired-Reason";
}
