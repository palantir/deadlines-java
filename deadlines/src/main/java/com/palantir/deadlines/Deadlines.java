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
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.TraceLocal;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

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

    private static Clock clock = System::nanoTime;

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
        ProvidedDeadline stateDeadline = deadlineState.get();
        if (stateDeadline == null) {
            return Optional.empty();
        }
        if (stateDeadline.disablePropagation()) {
            return Optional.empty();
        }
        long remaining = stateDeadline.remainingNanos(getClockNanoTime());
        return Optional.of(remaining <= 0 ? Duration.ZERO : Duration.ofNanos(remaining));
    }

    /**
     * Disables propagation of deadline values any further for the current trace.
     *
     * Callers can use this to short-circuit deadline propagation from the current trace when they are sure that
     * further operations should not be subject to deadline enforcement.
     *
     * Further calls to {@link #encodeToRequest} will result in a no-op assuming a deadline has previously been
     * set for this trace (e.g. via a previous call to {@link #parseFromRequest}).
     *
     * Further calls to {@link #getRemainingDeadline} will return {@link Optional#empty()}.
     */
    public static void disableFurtherDeadlinePropagation() {
        ProvidedDeadline currentState = deadlineState.get();
        if (currentState != null && !currentState.disablePropagation()) {
            // does not check for expiration
            deadlineState.set(new ProvidedDeadline(
                    currentState.valueNanos(),
                    currentState.wallClockNanos(),
                    currentState.internal(),
                    true,
                    currentState.enforced()));
        }
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
     * The enforcement flag controls whether this method should check the current deadline with enforcement, and
     * whether an enforcement flag should be added to outbound headers as well. The actual value selected for the
     * flag is dependent upon the context in which this method is called:
     *   - if there is _no_ existing deadline stored in state, then the caller is assumed to be the "originator",
     *     and enforcement is enabled if the {@code enforced} flag on this method call is true.
     *   - if there _is_ an existing deadline stored in state, then the flag's value is taken from the stored
     *     enforcement flag, regardless of the value of the {@code enforced} parameter to this method.
     * The latter case is important, as we want to respect the enforcement flag provided by a caller further up
     * in the RPC call chain even if the local caller about to send a request wants enforcement enabled (if a previous
     * RPC call in the chain requested no enforcement, we might be stepping on their toes).
     *
     * When enforcement is determined to be enabled (either via the {@code enforced} parameter or via a flag already
     * stored in deadline state), this method may throw a {@link DeadlineExpiredException} if, at the time of call,
     * the selected deadline has already expired.
     *
     * This function has no side-effects on the internal deadline state stored in a TraceLocal.
     *
     * @param proposedDeadline a proposed value for the deadline; the actual value used will be the minimum of
     * this value and one already set via a previous call to {@link #parseFromRequest}, if it exists
     * @param request the request object to write the encoding to
     * @param adapter a {@link RequestEncodingAdapter} that handles writing the header value to the request object
     * @param enforced enforce deadline expiration on this call, and propagate enforcement via a header as well
     */
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter, boolean enforced) {
        ProvidedDeadline stateDeadline = deadlineState.get();
        long proposedDeadlineNanos = proposedDeadline.toNanos();
        if (stateDeadline == null) {
            // use proposedDeadline
            checkExpiration(
                    proposedDeadlineNanos,
                    false,
                    false,
                    false,
                    // we have no existing deadline in state, so the caller can dictate whether this
                    // deadline should be enforced by receivers
                    // this allows originators (the first to send a request with an Expect-Within header)
                    // to control enforcement (and propagation of that enforcement) further along the call chain
                    enforced);
            adapter.setHeader(
                    request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
            if (enforced) {
                adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "true");
            }
        } else {
            // use the minimum of proposedDeadline and the one read from state
            long remainingStateDeadlineNanos = stateDeadline.remainingNanos(getClockNanoTime());
            // in cases where we do have an existing deadline in state, use the enforcement flag already stored,
            // and ignore the parameter passed here.
            // this means that even if callers _want_ to trigger enforcement, they cannot if they are propagating
            // an existing deadline from an upstream RPC call chain (unless _that_ caller requested enforcement)
            boolean enforcedFromState = stateDeadline.enforced();
            if (proposedDeadlineNanos <= remainingStateDeadlineNanos) {
                boolean proposedDeadlineAlreadyExpired = proposedDeadline.isNegative() || proposedDeadline.isZero();
                checkExpiration(
                        proposedDeadlineNanos,
                        false,
                        stateDeadline.disablePropagation(),
                        proposedDeadlineAlreadyExpired,
                        enforcedFromState);
                if (!stateDeadline.disablePropagation()) {
                    adapter.setHeader(
                            request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
                    if (enforcedFromState) {
                        adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "true");
                    }
                }
            } else {
                checkExpiration(
                        remainingStateDeadlineNanos,
                        stateDeadline.internal(),
                        stateDeadline.disablePropagation(),
                        stateDeadline.alreadyExpired(),
                        enforcedFromState);
                if (!stateDeadline.disablePropagation()) {
                    adapter.setHeader(
                            request,
                            DeadlinesHttpHeaders.EXPECT_WITHIN,
                            durationToHeaderValue(remainingStateDeadlineNanos));
                    if (enforcedFromState) {
                        adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, "true");
                    }
                }
            }
        }
    }

    // for backcompat, no originator enforcement
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        encodeToRequest(proposedDeadline, request, adapter, false);
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
        Long headerDeadline =
                tryParseSecondsToNanoseconds(adapter.maybeFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN));
        String headerEnforced = adapter.maybeFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED);
        boolean enforced = headerEnforced != null && headerEnforced.equalsIgnoreCase("true");
        if (headerDeadline != null && internalDeadline.isEmpty()) {
            // use the deadline parsed from a header, which is considered external
            storeDeadline(headerDeadline, false, enforced);
        } else if (headerDeadline == null && internalDeadline.isPresent()) {
            // use the deadline provided to this method, which is considered internal
            storeDeadline(
                    internalDeadline.get().toNanos(),
                    true,
                    false /* for the time being, internally-realized deadlines are never enforced */);
        } else if (headerDeadline != null) {
            // both present, so use the one that's lower
            long internalDeadlineValue = internalDeadline.get().toNanos();
            if (headerDeadline <= internalDeadlineValue) {
                storeDeadline(headerDeadline, false, enforced);
            } else {
                storeDeadline(
                        internalDeadlineValue,
                        true,
                        // false /* for the time being, internally-realized deadlines are never enforced */);
                        enforced);
            }
        }

        // no-op if neither header is present nor optional internalDeadline is present
    }

    private static void storeDeadline(long deadline, boolean internal, boolean enforced) {
        ProvidedDeadline providedDeadline =
                new ProvidedDeadline(deadline, getClockNanoTime(), internal, false, enforced);
        deadlineState.set(providedDeadline);
    }

    private static void checkExpiration(
            long deadline, boolean internal, boolean disablePropagation, boolean alreadyExpired, boolean enforced) {
        if (deadline <= 0) {
            // expired
            Expired_Cause cause = internal ? Expired_Cause.INTERNAL : Expired_Cause.EXTERNAL;
            Expired_Intent intent = Expired_Intent.PROPAGATE;
            if (disablePropagation) {
                intent = Expired_Intent.IGNORE;
            } else if (alreadyExpired) {
                intent = Expired_Intent.PROPAGATE_ALREADY_EXPIRED;
            }
            metrics.expired().cause(cause).intent(intent).build().mark();
            if (enforced) {
                throw internal ? DeadlineExpiredException.internal() : DeadlineExpiredException.external();
            }
        }
    }

    // converts nanoseconds to a String representing seconds (or fractions thereof)
    // example:
    //     durationToHeaderValue(1523000000L)
    // returns "1.523"
    @VisibleForTesting
    static String durationToHeaderValue(long durationNanos) {
        if (durationNanos <= 0) {
            // avoid incorrectly encoding negative values
            return "0";
        }
        // this algorithm's precision only affords up to milliseconds, so we take the ceiling of the millis value.
        // this helps avoid pathological scenarios where a deadline is propoagated through a deep call stack
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
    @VisibleForTesting
    static Long tryParseSecondsToNanoseconds(@Nullable String value) {
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

    @VisibleForTesting
    static void setClock(Clock newClock) {
        clock = newClock;
    }

    private static long getClockNanoTime() {
        return clock.nanoTime();
    }

    public interface RequestEncodingAdapter<REQUEST> {
        void setHeader(REQUEST request, String headerName, String headerValue);
    }

    public interface RequestDecodingAdapter<REQUEST> {
        @Deprecated
        Optional<String> getFirstHeader(REQUEST request, String headerName);

        @Nullable
        default String maybeFirstHeader(REQUEST request, String headerName) {
            return getFirstHeader(request, headerName).orElse(null);
        }
    }

    private record ProvidedDeadline(
            long valueNanos, long wallClockNanos, boolean internal, boolean disablePropagation, boolean enforced) {
        long remainingNanos(long currentWallClockNanos) {
            long elapsed = currentWallClockNanos - this.wallClockNanos;
            return valueNanos - elapsed;
        }

        boolean alreadyExpired() {
            return valueNanos <= 0;
        }
    }

    interface Clock {
        long nanoTime();
    }
}
