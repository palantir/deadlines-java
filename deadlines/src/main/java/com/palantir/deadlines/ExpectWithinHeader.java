/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import javax.annotation.Nullable;

/**
 * Wire codec for the {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header value, which is a decimal number of seconds.
 * <p>
 * {@link #format} and {@link #parse} are each other's inverse up to millisecond precision, and are kept together so
 * that the rounding behaviour of one can be checked against the other.
 */
final class ExpectWithinHeader {

    private static final SafeLogger log = SafeLoggerFactory.get(ExpectWithinHeader.class);
    private static final RateLimiter logLimiter = RateLimiter.create(1.0);

    private static final CharMatcher decimalMatcher =
            CharMatcher.inRange('0', '9').or(CharMatcher.is('.')).precomputed();

    private ExpectWithinHeader() {}

    /**
     * Converts nanoseconds to a String representing seconds (or fractions thereof).
     * <p>
     * For example {@code format(1523000000L)} returns {@code "1.523"}.
     */
    static String format(long durationNanos) {
        if (durationNanos <= 0) {
            // avoid incorrectly encoding negative values
            return "0";
        }
        // this algorithm's precision only affords up to milliseconds, so we take the ceiling of the millis value.
        // this helps avoid pathological scenarios where a deadline is propagated through a deep call stack
        // very quickly (at sub-millisecond precision); without adjusting to the ceiling, we would potentially
        // lose at least 1ms off the deadline on each RPC call which might cause request chains to abort
        // even though very little wall-clock time had elapsed.
        // instead, rounding the milliseconds protects against that scenario, at the expense of possibly
        // allowing requests with a deadline of < 1ms (or, very close to expiring) to continue.
        //
        // take the ceiling on the milliseconds here, and also avoid a potential overflow scenario;
        // if we are close to Long.MAX_VALUE, the deadline is already very, very large and there's not much
        // reason to round up to the nearest millisecond anyway.
        long ceilingMilliNanos = durationNanos < (Long.MAX_VALUE - 999999 /*precomputed by the compiler*/)
                ? durationNanos + 999999
                : durationNanos;
        return (ceilingMilliNanos / 1000000000)
                + "."
                + ((int) (ceilingMilliNanos % 1000000000) / 100000000)
                + ((int) (ceilingMilliNanos % 100000000) / 10000000)
                + ((int) (ceilingMilliNanos % 10000000) / 1000000);
    }

    /**
     * Parses a String representing seconds (or fractions thereof) to nanoseconds; otherwise null if invalid.
     */
    @Nullable
    static Long parse(@Nullable String value) {
        if (value == null) {
            return null;
        }
        NumberFormatException exception = null;
        String normalized = Strings.nullToEmpty(value).trim();
        if (!normalized.isEmpty() && decimalMatcher.matchesAllOf(normalized)) {
            try {
                double seconds = Double.parseDouble(normalized);
                return (long) (seconds * 1e9d);
            } catch (NumberFormatException e) {
                exception = e;
            }
        }

        if (log.isWarnEnabled()) {
            if (logLimiter.tryAcquire()) {
                log.warn("Failed to parse 'Expect-Within' header value", SafeArg.of("value", value));
            } else if (log.isDebugEnabled()) {
                log.debug("Failed to parse 'Expect-Within' header value", SafeArg.of("value", value), exception);
            }
        }
        return null;
    }
}
