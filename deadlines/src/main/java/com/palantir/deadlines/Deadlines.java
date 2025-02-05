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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.TraceLocal;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility methods for working with deadlines.
 */
public final class Deadlines {

    private static final SafeLogger log = SafeLoggerFactory.get(Deadlines.class);
    private static final RateLimiter logLimiter = RateLimiter.create(1.0);

    private Deadlines() {}

    private static final TraceLocal<ProvidedDeadline> deadlineState = TraceLocal.of();
    private static final DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
    private static final CharMatcher decimalMatcher =
            CharMatcher.inRange('0', '9').or(CharMatcher.is('.')).precomputed();

    /**
     * Get the amount of time remaining for the current deadline.
     *
     * Queries the current deadline state from a TraceLocal, and returns a {@link Duration} for the
     * amount of time remaining towards that deadline. If the deadline has already expired, then
     * {@link Duration#ZERO} is returned.
     *
     * If no deadline state has been set for the current trace, return an empty Optional.
     *
     * @return the remaining deadline time for the current trace, or {@link Duration#ZERO} if the deadline
     * has expired, or {@link Optional#empty()} if no such deadline state exists.
     */
    public static Optional<Duration> getRemainingDeadline() {
        Optional<RemainingDeadline> remainingDeadline = getRemainingDeadlineInternal();
        if (remainingDeadline.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(remainingDeadline.get().asDuration());
    }

    private static Optional<RemainingDeadline> getRemainingDeadlineInternal() {
        ProvidedDeadline providedDeadline = deadlineState.get();
        if (providedDeadline == null) {
            return Optional.empty();
        }
        // compute the remaining deadline relative to the current wall clock (may be negative)
        long elapsed = System.nanoTime() - providedDeadline.wallClockNanos();
        long remaining = providedDeadline.valueNanos() - elapsed;
        return Optional.of(new RemainingDeadline(remaining, providedDeadline.internal()));
    }

    /**
     * Encode a deadline into a request header.
     *
     * The actual deadline value encoded will be the minimum of:
     *   - the providedDeadline parameter
     *   - the value returned by {@link #getRemainingDeadline()}} if it exists
     * This ensures that the deadline set for the request will be based on the remaining deadline from
     * already-set internal state, or a smaller one if the caller chooses that.
     *
     * This function has no side-effects on the internal deadline state stored in a TraceLocal.
     *
     * @param proposedDeadline a proposed value for the deadline; the actual value used will be the minimum of
     * this value and one already set via a previous call to {@link #parseFromRequest}, if it exists
     * @param request the request object to write the encoding to
     * @param adapter a {@link RequestEncodingAdapter} that handles writing the header value to the request object
     */
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        Optional<RemainingDeadline> deadlineFromState = getRemainingDeadlineInternal();
        long proposedDeadlineNanos = proposedDeadline.toNanos();
        if (deadlineFromState.isEmpty()) {
            // use proposedDeadline
            checkExpiration(proposedDeadlineNanos, false);
            adapter.setHeader(
                    request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
        } else {
            // use the minimum of proposedDeadline and the one read from state
            RemainingDeadline stateDeadline = deadlineFromState.get();
            if (proposedDeadlineNanos <= stateDeadline.valueNanos()) {
                checkExpiration(proposedDeadlineNanos, false);
                adapter.setHeader(
                        request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
            } else {
                checkExpiration(stateDeadline.valueNanos(), stateDeadline.internal());
                adapter.setHeader(
                        request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(stateDeadline.valueNanos()));
            }
        }
    }

    /**
     * Parse a deadline value from a request header and set the deadline state for the current trace.
     *
     * If the request object contains a deadline value in a header, this method will parse it and store
     * the deadline value state internally in a TraceLocal, making it available to future calls to
     * {@link #getRemainingDeadline()}} from threads participating in the current trace. The deadline value
     * is read from a {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header on the request object.
     *
     * This function has side-effects on the internal deadline state stored in a TraceLocal; the state is
     * set (or overwritten) based on the value of the deadline parsed from request headers.
     *
     * @param internalDeadline if present, represents an alternative deadline that should be used if it is
     * lower than the one parsed from a request header
     * @param request the request object to read the deadline value from
     * @param adapter a {@link RequestDecodingAdapter} that handles reading the header value from the request object
     */
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline, T request, RequestDecodingAdapter<? super T> adapter) {
        Optional<Long> headerDeadline = adapter.getFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN)
                .map(Deadlines::tryParseSecondsToNanoseconds);

        if (headerDeadline.isPresent() && internalDeadline.isEmpty()) {
            // use the deadline parsed from a header, which is considered external
            storeDeadline(headerDeadline.get(), false);
        } else if (headerDeadline.isEmpty() && internalDeadline.isPresent()) {
            // use the deadline provided to this method, which is considered internal
            storeDeadline(internalDeadline.get().toNanos(), true);
        } else if (headerDeadline.isPresent()) {
            // both present, so use the one that's lower
            long headerDeadlineValue = headerDeadline.get();
            long internalDeadlineValue = internalDeadline.get().toNanos();
            if (headerDeadlineValue <= internalDeadlineValue) {
                storeDeadline(headerDeadlineValue, false);
            } else {
                storeDeadline(internalDeadlineValue, true);
            }
        }

        // no-op if neither header is present nor optional internalDeadline is present
    }

    private static void storeDeadline(long deadline, boolean internal) {
        checkExpiration(deadline, internal);
        ProvidedDeadline providedDeadline = new ProvidedDeadline(deadline, System.nanoTime(), internal);
        deadlineState.set(providedDeadline);
    }

    private static void checkExpiration(long deadline, boolean internal) {
        if (deadline <= 0) {
            // expired
            Expired_Cause cause = internal ? Expired_Cause.INTERNAL : Expired_Cause.EXTERNAL;
            metrics.expired(cause).mark();
            // TODO(blaub): throw exception instead of return
        }
    }

    // converts nanoseconds to a String representing seconds (or fractions thereof)
    // example:
    //     durationToHeaderValue(1523000000L)
    // returns "1.523"
    @VisibleForTesting
    static String durationToHeaderValue(long durationNanos) {
        // avoid operations on double and String.format
        return (durationNanos / 1000000000)
                + "."
                + ((int) (durationNanos % 1000000000) / 100000000)
                + ((int) (durationNanos % 100000000) / 10000000)
                + ((int) (durationNanos % 10000000) / 1000000);
    }

    /**
     * Parses a String representing seconds (or fractions thereof) to nanoseconds; otherwise null if invalid.
     */
    @Nullable
    @VisibleForTesting
    static Long tryParseSecondsToNanoseconds(String value) {
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

    public interface RequestEncodingAdapter<REQUEST> {
        void setHeader(REQUEST request, String headerName, String headerValue);
    }

    public interface RequestDecodingAdapter<REQUEST> {
        Optional<String> getFirstHeader(REQUEST request, String headerName);
    }

    private record ProvidedDeadline(long valueNanos, long wallClockNanos, boolean internal) {}

    private record RemainingDeadline(long valueNanos, boolean internal) {
        Duration asDuration() {
            return valueNanos <= 0 ? Duration.ZERO : Duration.ofNanos(valueNanos);
        }
    }
}
