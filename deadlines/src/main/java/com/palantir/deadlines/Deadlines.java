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

import com.palantir.deadlines.api.DeadlinesHttpHeaders;
import com.palantir.tracing.TraceLocal;
import java.time.Duration;
import java.util.Optional;

/**
 * Utility methods for working with deadlines.
 */
public class Deadlines {

    private static final TraceLocal<ProvidedDeadline> deadlineState = TraceLocal.of();

    public static void putDeadline(Duration deadline) {
        // store deadline state in a TraceLocal
        // what do we do if there is an existing state stored in the TraceLocal already??
        ProvidedDeadline providedDeadline = new ProvidedDeadline(deadline.toNanos(), System.nanoTime());
        deadlineState.set(providedDeadline);
    }

    public static Optional<Duration> getRemainingDeadline() {
        // retrieve the current deadline state from the TraceLocal, then
        // compute the remaining deadline based on the current wall clock
        // should return empty if no state is stored
        ProvidedDeadline providedDeadline = deadlineState.get();
        if (providedDeadline == null) {
            return Optional.empty();
        }
        // compute the remaining deadline relative to the current wall clock (may be negative)
        long elapsed = System.nanoTime() - providedDeadline.wallClockNanos();
        long remaining = providedDeadline.valueNanos() - elapsed;
        return Optional.of(Duration.ofNanos(remaining));
    }

    public static <T> void encodeToRequest(Duration deadline, T request, RequestEncodingAdapter<? super T> adapter) {
        adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(deadline));
    }

    public static <T> Optional<Duration> parseFromRequest(T request, RequestDecodingAdapter<? super T> adapter) {
        Optional<String> maybeExpectWithin = adapter.getFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN);
        if (maybeExpectWithin.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(headerValueToDuration(maybeExpectWithin.get()));
    }

    // converts a Duration to a String representing seconds (or fractions thereof)
    // example:
    //     durationToHeaderValue(Duration.ofNanos(1.5E9))
    // returns "1.5"
    private static String durationToHeaderValue(Duration value) {
        double seconds = (double) value.toNanos() / 1e9d;
        return String.format("%.5f", seconds);
    }

    // converts a String representing seconds (or fractions thereof) to a Duration
    private static Duration headerValueToDuration(String value) {
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
