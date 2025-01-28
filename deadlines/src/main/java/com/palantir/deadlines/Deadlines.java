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
     * amount of time remaining towards that deadline.
     *
     * If no deadline state has been set for the current trace, return an empty Optional.
     *
     * @return the remaining deadline time for the current trace, or {@link Optional#empty()} if
     * no such deadline state exists.
     */
    public static Optional<Duration> getRemainingDeadline() {
        ProvidedDeadline providedDeadline = deadlineState.get();
        if (providedDeadline == null) {
            return Optional.empty();
        }
        // compute the remaining deadline relative to the current wall clock (may be negative)
        long elapsed = System.nanoTime() - providedDeadline.wallClockNanos();
        long remaining = providedDeadline.valueNanos() - elapsed;
        return Optional.of(Duration.ofNanos(remaining));
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
     */
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        Duration actualDeadline = proposedDeadline;
        Optional<Duration> deadlineFromState = getRemainingDeadline();
        if (deadlineFromState.isPresent() && deadlineFromState.get().compareTo(proposedDeadline) < 0) {
            actualDeadline = deadlineFromState.get();
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
     * If no such header exists, an {@link Optional#empty()} is returned.
     *
     * This function has side-effects on the internal deadline state stored in a TraceLocal; the state is
     * set (or overwritten) based on the value of the deadline parsed from request headers.
     *
     * @param request the request object to read the deadline value from
     * @param adapter a {@link RequestDecodingAdapter} that handles reading the header value from the request object
     * @return the remaining deadline time parsed from the request headers, or {@link Optional#empty()} if
     * no such deadline exists.
     */
    public static <T> Optional<Duration> parseFromRequest(T request, RequestDecodingAdapter<? super T> adapter) {
        Optional<String> maybeExpectWithin = adapter.getFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN);
        if (maybeExpectWithin.isEmpty()) {
            return Optional.empty();
        }
        Duration deadlineValue = headerValueToDuration(maybeExpectWithin.get());
        // store deadline state in a TraceLocal
        // note that this overwrites any existing values, but that's okay since we assume this method
        // will be called at the beginning of processing an RPC call, where there should be _no_ existing deadline yet.
        ProvidedDeadline providedDeadline = new ProvidedDeadline(deadlineValue.toNanos(), System.nanoTime());
        deadlineState.set(providedDeadline);

        // don't call getRemainingDeadline(), which would return something slightly smaller than what was provided
        // in the header as some amount of processing time has elapsed.
        // this is potentially debatable, but I don't think callers should incur a penalty due to processing
        // internal to this library only. Instead, just return exactly what was provided on the wire.
        // TODO(blaub): this is a bit weird to return a value when deadlineState.set() may do nothing if we
        // are called outside of a trace. Perhaps it would make more sense for this method to return void and have
        // the caller explicitly query the state via getRemainingDeadline()
        return Optional.of(deadlineValue);
    }

    // converts a Duration to a String representing seconds (or fractions thereof)
    // example:
    //     durationToHeaderValue(Duration.ofNanos(1.5E9))
    // returns "1.5"
    @VisibleForTesting
    static String durationToHeaderValue(Duration value) {
        // TODO(blaub): fix this to avoid double math and string formatting
        // see:
        // https://github.palantir.build/foundry/witchcraft/blob/3275955c72688a32c5e51442962f6cf384b743a4/witchcraft-core/src/main/java/com/palantir/witchcraft/ServerTimingHandler.java#L47-L57
        double seconds = (double) value.toNanos() / 1e9d;
        return String.format("%.5f", seconds);
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
