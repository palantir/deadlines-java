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
import com.google.errorprone.annotations.InlineMe;
import com.google.errorprone.annotations.MustBeClosed;
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

    private static final ThreadLocal<ProvidedDeadline> threadLocalOverride = new ThreadLocal<>();

    @SuppressWarnings("for-rollout:deprecation")
    private static final DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());

    private static final CharMatcher decimalMatcher =
            CharMatcher.inRange('0', '9').or(CharMatcher.is('.')).precomputed();

    private static Clock clock = System::nanoTime;

    /**
     * Get the amount of time remaining for the current deadline.
     * <p>
     * Queries the current deadline state from a TraceLocal, and returns a {@link Duration} for the
     * amount of time remaining towards that deadline. If the deadline has already expired, then
     * {@link Duration#ZERO} is returned.
     * <p>
     * If no deadline state has been set for the current trace, return an empty Optional.
     * <p>
     * If the current thread is within a {@link ScopedDeadline} created by {@link #withDeadline(Duration, Enforcement)}
     * or {@link #withoutDeadline()}, the scoped deadline will be used instead of the TraceLocal state.
     *
     * @return the remaining deadline time for the current trace, or {@link Duration#ZERO} if the deadline
     * has expired, or {@link Optional#empty()} if no such deadline state exists.
     */
    public static Optional<Duration> getRemainingDeadline() {
        // Check thread-local override first
        ProvidedDeadline override = threadLocalOverride.get();
        ProvidedDeadline stateDeadline = override != null ? override : deadlineState.get();
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
     * <p>
     * Callers can use this to short-circuit deadline propagation from the current trace when they are sure that
     * further operations should not be subject to deadline enforcement.
     * <p>
     * Further calls to {@link #encodeToRequest} will result in a no-op assuming a deadline has previously been
     * set for this trace (e.g. via a previous call to {@link #parseFromRequest}).
     * <p>
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
                    // set the enforcement to DEFER to avoid having checkExpiration throw
                    Enforcement.DEFER));
        }
    }

    /**
     * Execute a code block with an alternative deadline, ignoring the current TraceLocal state.
     * <p>
     * The alternative deadline will only affect the current thread for the duration of the
     * returned scope. When the scope closes, the thread reverts to using the TraceLocal state.
     * <p>
     * This is useful when you need to execute an operation with a different deadline than the
     * one currently in effect for the trace. For example, you might want to give a retry operation
     * a shorter deadline than the overall request deadline.
     * <p>
     * Example usage:
     * <pre>
     * try (var scope = Deadlines.withDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE)) {
     *     // This operation uses the 5-second deadline regardless of TraceLocal state
     *     service.performOperation();
     * }
     * // Back to TraceLocal state
     * </pre>
     *
     * @param deadline the alternative deadline duration
     * @param enforcement the enforcement strategy for the alternative deadline
     * @return a ScopedDeadline that must be closed to restore previous state
     * @see #withDeadline(Duration)
     */
    @MustBeClosed
    public static ScopedDeadline withDeadline(Duration deadline, Enforcement enforcement) {
        long deadlineNanos = deadline.toNanos();
        ProvidedDeadline override = new ProvidedDeadline(deadlineNanos, getClockNanoTime(), true, false, enforcement);
        return new ThreadLocalScopedDeadline(override);
    }

    /**
     * Execute a code block with an alternative deadline, inheriting the enforcement strategy
     * from the current TraceLocal state.
     * <p>
     * This overload provides a convenient way to set a different deadline duration while
     * preserving the enforcement strategy that's currently in effect. If there is no deadline
     * state in the TraceLocal, the enforcement strategy defaults to {@link Enforcement#DEFER}.
     * <p>
     * The alternative deadline will only affect the current thread for the duration of the
     * returned scope. When the scope closes, the thread reverts to using the TraceLocal state.
     * <p>
     * Example usage:
     * <pre>
     * try (ScopedDeadline scope = Deadlines.withDeadline(Duration.ofSeconds(5))) {
     *     // This operation uses the 5-second deadline with the current enforcement strategy
     *     service.performOperation();
     * }
     * // Back to TraceLocal state
     * </pre>
     *
     * @param deadline the alternative deadline duration
     * @return a ScopedDeadline that must be closed to restore previous state
     * @see #withDeadline(Duration, Enforcement)
     */
    @MustBeClosed
    public static ScopedDeadline withDeadline(Duration deadline) {
        // Determine enforcement from TraceLocal state, or default to DEFER
        ProvidedDeadline traceLocalState = deadlineState.get();
        Enforcement enforcement = traceLocalState != null ? traceLocalState.enforcement() : Enforcement.DEFER;
        return withDeadline(deadline, enforcement);
    }

    /**
     * Execute a code block without deadline constraints, ignoring the current TraceLocal state.
     * <p>
     * This is useful for operations that should never be subject to deadline enforcement,
     * such as cleanup routines or logging operations that must complete regardless of the
     * deadline state.
     * <p>
     * When this scope is active, calls to {@link #getRemainingDeadline()} will return
     * {@link Optional#empty()}, and calls to {@link #encodeToRequest} will not propagate
     * deadline information to downstream services.
     * <p>
     * Example usage:
     * <pre>
     * try (var scope = Deadlines.withoutDeadline()) {
     *     // Cleanup operation not subject to deadlines
     *     cleanupService.performCleanup();
     * }
     * // Back to TraceLocal state
     * </pre>
     *
     * @return a ScopedDeadline that must be closed to restore previous state
     */
    @MustBeClosed
    public static ScopedDeadline withoutDeadline() {
        // Use a special marker: disablePropagation=true means "no deadline"
        ProvidedDeadline override =
                new ProvidedDeadline(Long.MAX_VALUE, getClockNanoTime(), true, true, Enforcement.DISABLE);
        return new ThreadLocalScopedDeadline(override);
    }

    /**
     * Encode a deadline into a request header.
     * <p>
     * The actual deadline value encoded will be the minimum of:
     *   - the providedDeadline parameter
     *   - the value returned by {@link #getRemainingDeadline()}} if it exists
     * This ensures that the deadline set for the request will be based on the remaining deadline from
     * already-set internal state, or a smaller one if the caller chooses that.
     * <p>
     * This function has no side effects on the internal deadline state stored in a TraceLocal.
     * <p>
     * If the current thread is within a {@link ScopedDeadline} created by {@link #withDeadline(Duration, Enforcement)}
     * or {@link #withoutDeadline()}, the scoped deadline will be used instead of the TraceLocal state.
     * <p>
     * The client requested enforcement strategy will be resolved against the internal state in the following manner:
     *   - If either client or internal state requests {@link Enforcement#DISABLE}, then the deadline is not enforced.
     *   - Then, if either client or internal state requests {@link Enforcement#ENFORCE}, then the deadline is enforced.
     *   - Finally, if both the client and internal state requests {@link Enforcement#DEFER}, the deadline is not
     *     enforced and a {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header is not encoded on the new request.
     *
     * @param proposedDeadline a proposed value for the deadline; the actual value used will be the minimum of
     * this value and one already set via a previous call to {@link #parseFromRequest}, if it exists
     * @param request the request object to write the encoding to
     * @param adapter a {@link RequestEncodingAdapter} that handles writing the header value to the request object
     * @param clientEnforcement a client requested {@link Enforcement} state, which will be resolved against any
     * existing state
     */
    public static <T> void encodeToRequest(
            Duration proposedDeadline,
            T request,
            RequestEncodingAdapter<? super T> adapter,
            Enforcement clientEnforcement) {
        // Check thread-local override first
        ProvidedDeadline override = threadLocalOverride.get();
        ProvidedDeadline stateDeadline = override != null ? override : deadlineState.get();
        long proposedDeadlineNanos = proposedDeadline.toNanos();
        if (stateDeadline == null) {
            // use proposedDeadline
            checkExpiration(proposedDeadlineNanos, false, false, false, clientEnforcement == Enforcement.ENFORCE);
            adapter.setHeader(
                    request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
            encodeEnforcement(request, adapter, clientEnforcement);
        } else {
            // use the minimum of proposedDeadline and the one read from state
            long remainingStateDeadlineNanos = stateDeadline.remainingNanos(getClockNanoTime());
            Enforcement resolvedEnforcement = stateDeadline.enforcement().resolveWith(clientEnforcement);
            boolean enforced = resolvedEnforcement == Enforcement.ENFORCE;
            if (proposedDeadlineNanos <= remainingStateDeadlineNanos) {
                boolean proposedDeadlineAlreadyExpired = proposedDeadline.isNegative() || proposedDeadline.isZero();
                checkExpiration(
                        proposedDeadlineNanos,
                        false,
                        stateDeadline.disablePropagation(),
                        proposedDeadlineAlreadyExpired,
                        enforced);
                if (!stateDeadline.disablePropagation()) {
                    adapter.setHeader(
                            request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(proposedDeadlineNanos));
                    encodeEnforcement(request, adapter, resolvedEnforcement);
                }
            } else {
                checkExpiration(
                        remainingStateDeadlineNanos,
                        stateDeadline.internal(),
                        stateDeadline.disablePropagation(),
                        stateDeadline.alreadyExpired(),
                        enforced);
                if (!stateDeadline.disablePropagation()) {
                    adapter.setHeader(
                            request,
                            DeadlinesHttpHeaders.EXPECT_WITHIN,
                            durationToHeaderValue(remainingStateDeadlineNanos));
                    encodeEnforcement(request, adapter, resolvedEnforcement);
                }
            }
        }
    }

    /**
     * Encode a deadline into a request header.
     * @deprecated Use {@link #encodeToRequest(Duration, Object, RequestEncodingAdapter, Enforcement)} instead
     */
    @Deprecated
    @InlineMe(
            replacement = "Deadlines.encodeToRequest(proposedDeadline, request, adapter, Enforcement.DEFER)",
            imports = {"com.palantir.deadlines.Deadlines", "com.palantir.deadlines.Deadlines.Enforcement"})
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        encodeToRequest(proposedDeadline, request, adapter, Enforcement.DEFER);
    }

    private static <T> void encodeEnforcement(
            T request, RequestEncodingAdapter<? super T> adapter, Enforcement enforcement) {
        String headerValue =
                switch (enforcement) {
                    case DISABLE -> "false";
                    case ENFORCE -> "true";
                    case DEFER -> null;
                };
        if (headerValue != null) {
            adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, headerValue);
        }
    }

    /**
     * Selects the enforcement strategy when parsing a deadline value from a request.
     * <p>
     * Enforcement controls whether this node should enforce expirations when they happen in a future call
     * to {@link #encodeToRequest} for the current trace. Enabling enforcement means that a {@link DeadlineExpiredException}
     * will be thrown when a deadline is detected to have expired, and that an enforcement flag will be propagated
     * when encoding a deadline for a new outbound request.
     */
    public enum Enforcement {
        /**
         * ENFORCE means that future calls to {@link #encodeToRequest} will throw an exception if the deadline has
         * expired, and also append an {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header with the value set
         * to "true" when encoding future deadlines from the current trace to request enforcement from downstream
         * nodes as well.
         * <p>
         * Note that calls to {@link #parseFromRequest} that receive an explicit {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED}
         * header with a value set to "false" will still override this to disable enforcement. This is intentional
         * to allow upstream nodes to short-circuit deadline enforcement if necessary.
         */
        ENFORCE,

        /**
         * DEFER means that future calls to {@link #encodeToRequest} MAY throw an exception if the deadline
         * has expired, but only if enforcement was requested at the time we parsed a deadline value from
         * a request header (e.g. if {@link #parseFromRequest} received an {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED}
         * header set to "true"). Otherwise, no deadline expiration enforcement will happen at this node, and we will
         * omit {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} headers on future outbound requests.
         * <p>
         * DEFER is used to indicate that we are deferring the enforcement strategy to either the inbound request,
         * or the next downstream hop, but will not enable enforcement here.
         */
        DEFER,

        /**
         * DISABLE means that deadline expiration will be ignored at this node, and ALL downstream nodes, regardless
         * of whether an {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header was received with the value
         * set to "true". Future calls to {@link #encodeToRequest} will not throw an exception if the deadline has
         * expired, and will set add a {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header on the request to
         * "false". This effectively terminates deadline enforcement at this node and causes downstream nodes to
         * ignore further enforcement for this trace, regardless of their configured enforcement state.
         */
        DISABLE;

        /**
         * Resolves two requested enforcement strategies against each other (client-requested, and internal deadline
         * state based on inbound request headers/service configuration) to produce a resulting enforcement strategy
         * which should be used when making outbound requests.
         */
        public Enforcement resolveWith(Enforcement other) {
            return switch (this) {
                case DISABLE -> this;
                case ENFORCE -> other.equals(Enforcement.DISABLE) ? other : this;
                case DEFER -> other;
            };
        }
    }

    private static Enforcement resolveEnforcementStrategy(
            @Nullable String headerEnforced, boolean headerDeadlineSet, Enforcement enforcementStrategy) {
        // check for the `Expect-Within-Enforced` flag in a header and set the outbound enforcement state
        // accordingly
        return getEnforcementFromHeaders(headerEnforced, headerDeadlineSet).resolveWith(enforcementStrategy);
    }

    private static Enforcement getEnforcementFromHeaders(@Nullable String headerEnforced, boolean headerDeadlineSet) {
        if (!headerDeadlineSet || headerEnforced == null) {
            return Enforcement.DEFER;
        }

        if (headerEnforced.equalsIgnoreCase("true")) {
            return Enforcement.ENFORCE;
        } else if (headerEnforced.equalsIgnoreCase("false")) {
            return Enforcement.DISABLE;
        } else {
            return Enforcement.DEFER;
        }
    }

    /**
     * Parse a deadline value from a request header and set the deadline state for the current trace.
     * <p>
     * If the request object contains a deadline value in a header, this method will parse it and store
     * the deadline value state internally in a TraceLocal, making it available to future calls to
     * {@link #getRemainingDeadline()}} from threads participating in the current trace. The deadline value
     * is read from a {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header on the request object.
     * <p>
     * Enforcement of the deadline is controlled in the following way:
     *   - If a {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header is parsed with a value of "false", then
     *     the deadline is not enforced and the value of "enforcementStrategy" is ignored
     *   - If the "enforcementStrategy" parameter is set to {@link Enforcement#DISABLE}, then it is not enforced.
     *   - If the "enforcementStrategy" parameter is set to {@link Enforcement#ENFORCE}, then the deadline state is
     *     set to {@link Enforcement#ENFORCE}
     *   - If the "enforcementStrategy" parameter is set to {@link Enforcement#DEFER}, then the deadline state is set to
     *     {@link Enforcement#ENFORCE} only if a {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header is parsed
     *     with a value of "true"
     * <p>
     * This function has side effects on the internal deadline state stored in a TraceLocal; the state is
     * set (or overwritten) based on the value of the deadline parsed from request headers. This state may eventually
     * be resolved against a client-provided enforcement strategy if an outbound request is made, for details on
     * resolution strategy, see {@link #encodeToRequest(Duration, Object, RequestEncodingAdapter, Enforcement)}
     *
     * @param internalDeadline if present, represents an alternative deadline that should be used if it is
     * lower than the one parsed from a request header
     * @param request the request object to read the deadline value from
     * @param adapter a {@link RequestDecodingAdapter} that handles reading the header value from the request object
     * @param enforcementStrategy configures enforcement strategy (see {@link Enforcement})
     */
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        Long headerDeadline =
                tryParseSecondsToNanoseconds(adapter.maybeFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN));
        String headerEnforced = adapter.maybeFirstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED);
        Enforcement stateEnforcement =
                resolveEnforcementStrategy(headerEnforced, headerDeadline != null, enforcementStrategy);
        if (headerDeadline != null) {
            if (internalDeadline.isEmpty()) {
                // use the deadline parsed from a header, which is considered external
                storeDeadline(headerDeadline, false, stateEnforcement);
            } else {
                // both present, so use the one that's lower
                long internalDeadlineValue = internalDeadline.get().toNanos();
                if (headerDeadline <= internalDeadlineValue) {
                    storeDeadline(headerDeadline, false, stateEnforcement);
                } else {
                    storeDeadline(internalDeadlineValue, true, stateEnforcement);
                }
            }
        } else if (internalDeadline.isPresent()) {
            // use the deadline provided to this method, which is considered internal
            storeDeadline(internalDeadline.get().toNanos(), true, stateEnforcement);
        }
        // no-op if neither header is present nor optional internalDeadline is present
    }

    @Deprecated
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline, T request, RequestDecodingAdapter<? super T> adapter) {
        // by default use DEFER, which matches behavior of consumers on older versions which have no enforcement
        parseFromRequest(internalDeadline, request, adapter, Enforcement.DEFER);
    }

    private static void storeDeadline(long deadline, boolean internal, Enforcement enforcement) {
        ProvidedDeadline providedDeadline =
                new ProvidedDeadline(deadline, getClockNanoTime(), internal, false, enforcement);
        deadlineState.set(providedDeadline);
    }

    private static void checkExpiration(
            long deadline, boolean internal, boolean disablePropagation, boolean alreadyExpired, boolean enforced) {
        if (deadline <= 0) {
            // expired
            Expired_Cause cause = internal ? Expired_Cause.INTERNAL : Expired_Cause.EXTERNAL;
            Expired_Intent intent;
            if (enforced) {
                // intent is always "throw" if enforced = true, regardless of what the other flags are
                intent = Expired_Intent.THROW;
            } else {
                // will not throw, report one of the other intents
                if (disablePropagation) {
                    intent = Expired_Intent.IGNORE;
                } else if (alreadyExpired) {
                    intent = Expired_Intent.PROPAGATE_ALREADY_EXPIRED;
                } else {
                    intent = Expired_Intent.PROPAGATE;
                }
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
            long valueNanos,
            long wallClockNanos,
            boolean internal,
            boolean disablePropagation,
            Enforcement enforcement) {
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

    /**
     * A scope that temporarily overrides the deadline state for the current thread.
     * <p>
     * Instances of this type are returned by {@link #withDeadline(Duration, Enforcement)} and
     * {@link #withoutDeadline()}. The scope must be closed to restore the previous deadline state.
     * <p>
     * Instances of this type are designed for use with try-with-resources:
     * <pre>
     * try (ScopedDeadline scope = Deadlines.withDeadline(Duration.ofSeconds(5), Enforcement.ENFORCE)) {
     *     // code here uses the alternative deadline
     * }
     * // previous deadline state restored
     * </pre>
     */
    public interface ScopedDeadline extends AutoCloseable {
        @Override
        void close();
    }

    private static final class ThreadLocalScopedDeadline implements ScopedDeadline {
        @Nullable
        private final ProvidedDeadline previousOverride;

        ThreadLocalScopedDeadline(@Nullable ProvidedDeadline newOverride) {
            this.previousOverride = threadLocalOverride.get();
            threadLocalOverride.set(newOverride);
        }

        @Override
        public void close() {
            threadLocalOverride.set(previousOverride);
        }
    }
}
