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
import com.palantir.requestcontext.RequestContext;
import com.palantir.requestcontext.RequestContextScope;
import com.palantir.tracing.TraceLocal;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Utility methods for working with deadlines.
 * <p>
 * The current deadline is determined as follows:
 * <ol>
 *   <li>If the current {@link RequestContext} has a deadline, it applies. That deadline may be "no deadline", as bound
 *       by {@link #withoutDeadline()} or by {@link #withRequestDeadline} for a request without one.
 *   <li>Otherwise, the deadline stored for the current trace by the deprecated
 *       {@link #parseFromRequest(Optional, Object, RequestDecodingAdapter, Enforcement)} applies, exactly as in
 *       previous releases.
 *   <li>Otherwise, there is no deadline.
 * </ol>
 * A deadline in the request context is handed to other threads with the context, for example with
 * {@code RequestContext.current().wrap(task)} or {@link RequestContext#propagating}. Tracing does not carry it.
 */
public final class Deadlines {

    private static final SafeLogger log = SafeLoggerFactory.get(Deadlines.class);
    private static final RateLimiter logLimiter = RateLimiter.create(1.0);

    private Deadlines() {}

    // The deadline in the request context. Absent means the context does not specify a deadline, so the legacy trace
    // state applies; ContextDeadline.NONE means the context has no deadline.
    private static final RequestContext.Key<ContextDeadline> CONTEXT_DEADLINE = RequestContext.Key.create("deadline");

    // Legacy deadline state, written only by deprecated methods. It is shared by every thread in a trace and applies
    // only when the current request context does not specify a deadline.
    private static final TraceLocal<ProvidedDeadline> deadlineState = TraceLocal.of();

    private static final Duration MAX_NANOS_DURATION = Duration.ofNanos(Long.MAX_VALUE);

    @SuppressWarnings("for-rollout:deprecation")
    private static final DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());

    private static final CharMatcher decimalMatcher =
            CharMatcher.inRange('0', '9').or(CharMatcher.is('.')).precomputed();

    private static Clock clock = System::nanoTime;

    /**
     * Get the amount of time remaining for the current deadline.
     * <p>
     * Returns a {@link Duration} for the amount of time remaining towards the current deadline (see the class
     * documentation). If the deadline has already expired, then {@link Duration#ZERO} is returned.
     * <p>
     * If there is no current deadline, return an empty Optional.
     *
     * @return the remaining time for the current deadline, or {@link Duration#ZERO} if the deadline
     * has expired, or {@link Optional#empty()} if there is no current deadline.
     */
    public static Optional<Duration> getRemainingDeadline() {
        ProvidedDeadline stateDeadline = currentState();
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
     * Throws a {@link DeadlineExpiredException} if the current deadline has expired and is enforced, recording
     * the expiration in the {@code deadline.expired} meter.
     * <p>
     * This is the counterpart to the check {@link #encodeToRequest} performs, for callers that need to enforce a
     * deadline at a point where they are not sending a request -- for example when work that was waiting on a
     * deadline-bounded budget is about to give up. It is a no-op when no deadline is present, when the deadline has
     * not yet expired, or when further propagation has been disabled.
     *
     * @param clientEnforcement the caller's requested strategy, resolved against the current deadline's strategy using
     *     {@link Enforcement#resolveWith(Enforcement)}
     */
    public static void checkDeadline(Enforcement clientEnforcement) {
        ProvidedDeadline stateDeadline = currentState();
        if (stateDeadline == null || stateDeadline.disablePropagation()) {
            // The deadline no longer applies to this trace, so its expiry is not an event worth reporting.
            return;
        }
        checkExpiration(
                stateDeadline.remainingNanos(getClockNanoTime()),
                stateDeadline.internal(),
                stateDeadline.disablePropagation(),
                stateDeadline.alreadyExpired(),
                stateDeadline.enforcement().resolveWith(clientEnforcement) == Enforcement.ENFORCE);
    }

    /**
     * Get the enforcement strategy for the current deadline.
     * <p>
     * Returns the {@link Enforcement} strategy of the current deadline (see the class documentation): the strategy
     * configured when the deadline was parsed, or inherited by {@link #withDeadline}.
     * <p>
     * If there is no current deadline, return an empty Optional.
     *
     * @return the enforcement strategy for the current deadline, or {@link Optional#empty()} if there is no current
     * deadline.
     */
    public static Optional<Enforcement> getEnforcement() {
        return Optional.ofNullable(currentState()).map(ProvidedDeadline::enforcement);
    }

    /**
     * Opens a scope in which the current deadline expires no later than {@code timeout} from now.
     * <p>
     * The scope binds the current request context with:
     * <ul>
     *   <li>the current deadline, unchanged, if there is one with no more than {@code timeout} remaining (including
     *       an exact tie);
     *   <li>otherwise, a new deadline that expires {@code timeout} from now. It is internal, so its expiration throws
     *       {@link DeadlineExpiredException.Internal}. It keeps the current deadline's {@link Enforcement}, or uses
     *       {@link Enforcement#DEFER} if there is no current deadline.
     * </ul>
     * A scope can therefore only shorten the deadline. To give a block of code more time than the current deadline
     * allows, open {@link #withoutDeadline()} and open this scope inside it.
     * <p>
     * A zero or negative {@code timeout} binds a deadline that has already expired. Opening the scope does not check
     * the deadline and never throws {@link DeadlineExpiredException}; call {@link #checkDeadline} to check it.
     * <p>
     * Close the scope on the thread that opened it, which restores the request context that was current before. Work
     * handed off from inside the scope with the request context keeps the scope's deadline after the scope closes.
     */
    @MustBeClosed
    public static RequestContextScope withDeadline(Duration timeout) {
        long timeoutNanos = timeoutToNanos(timeout);
        long now = getClockNanoTime();
        ProvidedDeadline current = currentState();
        ProvidedDeadline bound;
        if (current == null || current.disablePropagation()) {
            bound = new ProvidedDeadline(timeoutNanos, now, true, false, Enforcement.DEFER);
        } else if (current.remainingNanos(now) <= timeoutNanos) {
            bound = current;
        } else {
            bound = new ProvidedDeadline(timeoutNanos, now, true, false, current.enforcement());
        }
        return RequestContext.current()
                .with(CONTEXT_DEADLINE, new ContextDeadline(bound))
                .attach();
    }

    /**
     * Opens a scope in which there is no deadline, regardless of any deadline in an enclosing scope or stored for the
     * current trace. The request context's other values are unchanged, and so is tracing.
     * <p>
     * Inside the scope, {@link #getRemainingDeadline()} and {@link #getEnforcement()} are empty, {@link #checkDeadline}
     * and {@link #awaitWithinDeadline} never throw {@link DeadlineExpiredException}, and {@link #encodeToRequest} sends
     * only the proposed deadline with the client's enforcement, as if no deadline had ever been set. Use this for work
     * that must not be bounded by the current deadline, such as a load shared by several callers or cleanup that must
     * run after the deadline expires.
     * <p>
     * Close the scope on the thread that opened it, which restores the request context that was current before. Work
     * handed off from inside the scope with the request context has no deadline.
     */
    @MustBeClosed
    public static RequestContextScope withoutDeadline() {
        return RequestContext.current()
                .with(CONTEXT_DEADLINE, ContextDeadline.NONE)
                .attach();
    }

    /**
     * Returns {@code context} with the deadline of an inbound request, without binding it. Bind the result for the
     * duration of the request with {@link RequestContext#attach()}.
     * <p>
     * The deadline, its origin and its enforcement are determined exactly as by
     * {@link #parseFromRequest(Optional, Object, RequestDecodingAdapter, Enforcement)}, with the deadline measured from
     * now. If the request has no valid {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header and {@code internalDeadline} is
     * empty, the result has no deadline, so code running with it does not see any deadline stored for the trace.
     *
     * @param context the context to add the deadline to, usually {@link RequestContext#empty()} for a new request
     * @param internalDeadline if present, used instead of the request's deadline if it is shorter
     * @param request the request object to read the deadline from
     * @param adapter reads header values from the request object
     * @param enforcementStrategy configures enforcement strategy (see {@link Enforcement})
     */
    public static <T> RequestContext withRequestDeadline(
            RequestContext context,
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        ProvidedDeadline parsed = parseState(internalDeadline, request, adapter, enforcementStrategy);
        return context.with(CONTEXT_DEADLINE, parsed == null ? ContextDeadline.NONE : new ContextDeadline(parsed));
    }

    /**
     * Waits for {@code future} to complete, but no longer than the current deadline allows when that deadline is
     * enforced.
     * <p>
     * The current deadline is enforced when its {@link Enforcement}, resolved against {@code clientEnforcement} by
     * {@link Enforcement#resolveWith}, is {@link Enforcement#ENFORCE}. In that case:
     * <ul>
     *   <li>if {@code future} has already completed, its result is returned (or its failure thrown), even if the
     *       deadline has expired;
     *   <li>otherwise, if the deadline has expired or expires before {@code future} completes, the expiration is
     *       recorded in the {@code deadline.expired} meter and a {@link DeadlineExpiredException} is thrown.
     * </ul>
     * Without an enforced deadline, this behaves exactly like {@link Future#get()}.
     * <p>
     * {@code future} is never cancelled, so this is safe for work shared between callers, such as a cache load: each
     * caller stops waiting at its own deadline while the shared work continues.
     *
     * @throws ExecutionException if {@code future} completed exceptionally, as {@link Future#get()}
     * @throws InterruptedException if the current thread was interrupted while waiting, as {@link Future#get()}
     * @throws java.util.concurrent.CancellationException if {@code future} was cancelled, as {@link Future#get()}
     */
    public static <V> V awaitWithinDeadline(Future<V> future, Enforcement clientEnforcement)
            throws ExecutionException, InterruptedException {
        ProvidedDeadline stateDeadline = currentState();
        if (future.isDone()
                || stateDeadline == null
                || stateDeadline.disablePropagation()
                || stateDeadline.enforcement().resolveWith(clientEnforcement) != Enforcement.ENFORCE) {
            return future.get();
        }
        long remainingNanos = stateDeadline.remainingNanos(getClockNanoTime());
        if (remainingNanos > 0) {
            try {
                return future.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                // the deadline expired while waiting; fall through to report it
            }
        }
        recordExpiration(stateDeadline.internal(), Expired_Intent.THROW, stateDeadline.valueNanos());
        throw stateDeadline.internal() ? DeadlineExpiredException.internal() : DeadlineExpiredException.external();
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
     * Further calls to {@link #getRemainingDeadline} will return {@link Optional#empty()}, and
     * {@link #checkDeadline} becomes a no-op.
     * <p>
     * This affects only the deadline stored for the trace. It has no effect where the current request context has a
     * deadline, for example one bound by {@link #withRequestDeadline}.
     *
     * @deprecated Use {@link #withoutDeadline()}, which applies to the enclosed block of code and to work handed off
     * from it with the request context, rather than to every thread in the trace.
     */
    @Deprecated
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
        ProvidedDeadline stateDeadline = currentState();
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
        String headerValue = switch (enforcement) {
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
     * @deprecated Use {@link #withRequestDeadline} and {@link RequestContext#attach()}, which bind the deadline for the
     * duration of a scope instead of storing it for every thread in the trace. Where the current request context has
     * a deadline, the deadline stored by this method does not apply.
     */
    @Deprecated
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        ProvidedDeadline parsed = parseState(internalDeadline, request, adapter, enforcementStrategy);
        if (parsed != null) {
            deadlineState.set(parsed);
        }
        // no-op if neither header is present nor optional internalDeadline is present
    }

    @Deprecated
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline, T request, RequestDecodingAdapter<? super T> adapter) {
        // by default use DEFER, which matches behavior of consumers on older versions which have no enforcement
        parseFromRequest(internalDeadline, request, adapter, Enforcement.DEFER);
    }

    // Parses the deadline that parseFromRequest stores, without storing it. Returns null if the request has no valid
    // deadline header and there is no internal deadline.
    @Nullable
    private static <T> ProvidedDeadline parseState(
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
                return newDeadline(headerDeadline, false, stateEnforcement);
            } else {
                // both present, so use the one that's lower
                long internalDeadlineValue = internalDeadline.get().toNanos();
                if (headerDeadline <= internalDeadlineValue) {
                    return newDeadline(headerDeadline, false, stateEnforcement);
                } else {
                    return newDeadline(internalDeadlineValue, true, stateEnforcement);
                }
            }
        } else if (internalDeadline.isPresent()) {
            // use the deadline provided to this method, which is considered internal
            return newDeadline(internalDeadline.get().toNanos(), true, stateEnforcement);
        }
        return null;
    }

    private static ProvidedDeadline newDeadline(long deadline, boolean internal, Enforcement enforcement) {
        return new ProvidedDeadline(deadline, getClockNanoTime(), internal, false, enforcement);
    }

    // The current deadline, as described in the class documentation. Null means there is no deadline.
    @Nullable
    private static ProvidedDeadline currentState() {
        ContextDeadline contextDeadline = RequestContext.current().get(CONTEXT_DEADLINE);
        return contextDeadline != null ? contextDeadline.state() : deadlineState.get();
    }

    // Converts a timeout to nanoseconds, treating a negative timeout as zero. Duration.toNanos throws beyond roughly
    // 292 years, so longer timeouts saturate to Long.MAX_VALUE, which is effectively unbounded.
    private static long timeoutToNanos(Duration timeout) {
        if (timeout.isNegative()) {
            return 0;
        }
        return timeout.compareTo(MAX_NANOS_DURATION) >= 0 ? Long.MAX_VALUE : timeout.toNanos();
    }

    private static void checkExpiration(
            long deadline, boolean internal, boolean disablePropagation, boolean alreadyExpired, boolean enforced) {
        if (deadline <= 0) {
            // expired
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

            // Record the original deadline budget bucket. If state exists, use the original
            // value stored at parse time; otherwise the deadline arg itself is the original budget.
            ProvidedDeadline state = currentState();
            long originalBudgetNanos = state != null ? state.valueNanos() : deadline;

            recordExpiration(internal, intent, originalBudgetNanos);

            if (enforced) {
                throw internal ? DeadlineExpiredException.internal() : DeadlineExpiredException.external();
            }
        }
    }

    private static void recordExpiration(boolean internal, Expired_Intent intent, long originalBudgetNanos) {
        metrics.expired()
                .cause(internal ? Expired_Cause.INTERNAL : Expired_Cause.EXTERNAL)
                .intent(intent)
                .budget(budgetBucket(originalBudgetNanos))
                .build()
                .mark();
    }

    private static DeadlineMetrics.Expired_Budget budgetBucket(long nanos) {
        if (nanos < 100_000_000L) {
            return DeadlineMetrics.Expired_Budget.SUB_100MS;
        } else if (nanos < 1_000_000_000L) {
            return DeadlineMetrics.Expired_Budget.SUB_1S;
        } else if (nanos < 10_000_000_000L) {
            return DeadlineMetrics.Expired_Budget.SUB_10S;
        } else if (nanos < 100_000_000_000L) {
            return DeadlineMetrics.Expired_Budget.SUB_100S;
        } else {
            return DeadlineMetrics.Expired_Budget.ABOVE_100S;
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

    // The value stored in the request context. A null state means the context has no deadline.
    private record ContextDeadline(@Nullable ProvidedDeadline state) {
        static final ContextDeadline NONE = new ContextDeadline(null);
    }

    interface Clock {
        long nanoTime();
    }
}
