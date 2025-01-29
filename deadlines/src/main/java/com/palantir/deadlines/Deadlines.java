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
import com.palantir.deadlines.api.DeadlineExpiredException;
import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.TraceLocal;
import java.time.Duration;
import java.util.Optional;

/**
 * Utility methods for working with deadlines.
 */
public final class Deadlines {

    private Deadlines() {}

    private static final TraceLocal<ProvidedDeadline> deadlineState = TraceLocal.of();

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
        ProvidedDeadline providedDeadline = deadlineState.get();
        if (providedDeadline == null) {
            return Optional.empty();
        }
        // compute the remaining deadline relative to the current wall clock (may be negative)
        long elapsed = System.nanoTime() - providedDeadline.wallClockNanos();
        long remaining = providedDeadline.valueNanos() - elapsed;
        return remaining <= 0 ? Optional.of(Duration.ZERO) : Optional.of(Duration.ofNanos(remaining));
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
     * @param proposedDeadline a proposed value for the deadline; the actual value used will be the minimum
     * @param request the request object to write the encoding to
     * @param adapter a {@link RequestEncodingAdapter} that handles writing the header value to the request object
     * @throws DeadlineExpiredException if the actual deadline selected (per the above rules) has already expired
     */
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        Duration actualDeadline = proposedDeadline;
        Optional<Duration> deadlineFromState = getRemainingDeadline();
        if (deadlineFromState.isPresent() && deadlineFromState.get().compareTo(proposedDeadline) < 0) {
            actualDeadline = deadlineFromState.get();
        }

        if (actualDeadline.isZero() || actualDeadline.isNegative()) {
            // expired
            // TODO(blaub): report metrics
            // TODO(blaub): throw exception instead of return
            return;
        }

        adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(actualDeadline));
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
     * @throws DeadlineExpiredException if the deadline parsed from the request is <= 0
     */
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline, T request, RequestDecodingAdapter<? super T> adapter) {
        Optional<Duration> headerDeadline = adapter.getFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN)
                .map(Deadlines::headerValueToDuration);

        if (headerDeadline.isEmpty() && internalDeadline.isEmpty()) {
            // nothing to do
            return;
        } else if (headerDeadline.isPresent() && internalDeadline.isEmpty()) {
            storeDeadline(headerDeadline.get(), false);
        } else if (headerDeadline.isEmpty()) {
            storeDeadline(internalDeadline.get(), true);
        } else {
            // both present, so use the one that's lower
            Duration headerDeadlineValue = headerDeadline.get();
            Duration internalDeadlineValue = internalDeadline.get();
            if (headerDeadlineValue.compareTo(internalDeadlineValue) <= 0) {
                storeDeadline(headerDeadlineValue, false);
            } else {
                storeDeadline(internalDeadlineValue, true);
            }
        }
    }

    private static void storeDeadline(Duration deadline, boolean internal) {
        if (deadline.isNegative() || deadline.isZero()) {
            // expired
            // TODO(blaub): report metrics
            // TODO(blaub): throw exception instead of return
            return;
        }
        ProvidedDeadline providedDeadline = new ProvidedDeadline(deadline.toNanos(), System.nanoTime(), internal);
        deadlineState.set(providedDeadline);
    }

    // converts a Duration to a String representing seconds (or fractions thereof)
    // example:
    //     durationToHeaderValue(Duration.ofNanos(1523000000L))
    // returns "1.523"
    @VisibleForTesting
    static String durationToHeaderValue(Duration value) {
        // adapted from:
        // https://github.palantir.build/foundry/witchcraft/blob/develop/witchcraft-core/src/main/java/com/palantir/witchcraft/ServerTimingHandler.java#L47-L57
        // to avoid operations on double and String.format
        long durationNanos = value.toNanos();
        return (durationNanos / 1000000000)
                + "."
                + ((int) (durationNanos % 1000000000) / 100000000)
                + ((int) (durationNanos % 100000000) / 10000000)
                + ((int) (durationNanos % 10000000) / 1000000);
    }

    // converts a String representing seconds (or fractions thereof) to a Duration
    @VisibleForTesting
    static Duration headerValueToDuration(String value) {
        double seconds = Double.parseDouble(value);
        long nanos = (long) (seconds * 1e9d);
        return Duration.ofNanos(nanos);
    }

    public interface RequestEncodingAdapter<REQUEST> {
        void setHeader(REQUEST request, String headerName, String headerValue);
    }

    public interface RequestDecodingAdapter<REQUEST> {
        Optional<String> getFirstHeader(REQUEST request, String headerName);
    }
}
