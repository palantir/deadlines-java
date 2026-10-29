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

package com.palantir.callcontext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.callcontext.DeadlineMetrics.Expired_Cause;
import com.palantir.callcontext.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.DeadlineExpiredException;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import com.palantir.deadlines.Deadlines.RequestEncodingAdapter;
import com.palantir.deadlines.DeadlinesHttpHeaders;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Utility methods for working with deadlines stored in the {@link CallContext}.
 * <p>
 * The current deadline is determined as follows:
 * <ol>
 *   <li>If the current {@link CallContext} has a deadline, it applies. That deadline may be "no deadline", as bound
 *       by {@link #withoutDeadline()} or by {@link #withRequestDeadline} for a request without one, or as left by
 *       {@link #disarmRequestDeadline}.
 *   <li>Otherwise, the deadline that {@link com.palantir.deadlines.Deadlines} stores for the current trace applies.
 *       {@link #getRemainingDeadline()}, {@link #getEnforcement()}, {@link #checkDeadline} and {@link #encodeToRequest}
 *       then behave exactly as their counterparts there. {@link #awaitWithinDeadline} ignores it.
 *   <li>Otherwise, there is no deadline.
 * </ol>
 * A deadline in the call context is handed to other threads with the context, for example with
 * {@code CallContext.current().wrap(task)} or {@link CallContext#propagating}. Tracing does not carry it.
 * <p>
 * {@link Enforcement}, the request adapters, the headers and {@link DeadlineExpiredException} are those of
 * {@link com.palantir.deadlines.Deadlines}, so callers handle a deadline the same way whichever stores it.
 */
public final class Deadlines {

    private static final SafeLogger log = SafeLoggerFactory.get(Deadlines.class);
    private static final RateLimiter logLimiter = RateLimiter.create(1.0);

    private Deadlines() {}

    // The deadline in the call context. Absent means the context does not specify a deadline, so the deadline stored
    // for the trace applies; ContextDeadline.NONE means the context has no deadline.
    private static final CallContext.Key<ContextDeadline> CONTEXT_DEADLINE = CallContext.Key.create("deadline");

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
        ContextDeadline contextDeadline = contextDeadline();
        if (contextDeadline == null) {
            return TraceDeadlines.getRemainingDeadline();
        }
        ProvidedDeadline deadline = contextDeadline.state();
        if (deadline == null) {
            return Optional.empty();
        }
        long remaining = deadline.remainingNanos(getClockNanoTime());
        return Optional.of(remaining <= 0 ? Duration.ZERO : Duration.ofNanos(remaining));
    }

    /**
     * Throws a {@link DeadlineExpiredException} if the current deadline has expired and is enforced, recording
     * the expiration in the {@code deadline.expired} meter.
     * <p>
     * This is the counterpart to the check {@link #encodeToRequest} performs, for callers that need to enforce a
     * deadline at a point where they are not sending a request -- for example when work that was waiting on a
     * deadline-bounded budget is about to give up. It is a no-op when there is no current deadline or it has not yet
     * expired.
     *
     * @param clientEnforcement the caller's requested strategy, resolved against the current deadline's strategy using
     *     {@link Enforcement#resolveWith(Enforcement)}
     */
    public static void checkDeadline(Enforcement clientEnforcement) {
        ContextDeadline contextDeadline = contextDeadline();
        if (contextDeadline == null) {
            TraceDeadlines.checkDeadline(clientEnforcement);
            return;
        }
        ProvidedDeadline deadline = contextDeadline.state();
        if (deadline != null) {
            checkExpiration(
                    deadline.remainingNanos(getClockNanoTime()),
                    deadline.internal(),
                    deadline.alreadyExpired(),
                    deadline.enforcement().resolveWith(clientEnforcement) == Enforcement.ENFORCE,
                    deadline.valueNanos());
        }
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
        ContextDeadline contextDeadline = contextDeadline();
        if (contextDeadline == null) {
            return TraceDeadlines.getEnforcement();
        }
        return Optional.ofNullable(contextDeadline.state()).map(ProvidedDeadline::enforcement);
    }

    /**
     * Opens a scope in which the current deadline expires no later than {@code timeout} from now.
     * <p>
     * The scope binds the current call context with:
     * <ul>
     *   <li>the current deadline, unchanged, if there is one with no more than {@code timeout} remaining (including
     *       an exact tie). If that is the deadline stored for the trace, the context is bound unchanged, so the
     *       trace's deadline keeps applying and only tracing carries it to other threads;
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
     * Close the scope on the thread that opened it, which restores the call context that was current before. Work
     * handed off from inside the scope with the call context keeps the scope's deadline after the scope closes.
     */
    @MustBeClosed
    public static CallContextScope withDeadline(Duration timeout) {
        long timeoutNanos = timeoutToNanos(timeout);
        long now = getClockNanoTime();
        ContextDeadline contextDeadline = contextDeadline();
        CallContext context = CallContext.current();
        Enforcement enforcement;
        AtomicBoolean disarmed;
        if (contextDeadline == null) {
            Optional<Duration> traceRemaining = TraceDeadlines.getRemainingDeadline();
            if (traceRemaining.isPresent() && traceRemaining.get().toNanos() <= timeoutNanos) {
                return context.attach();
            }
            enforcement = TraceDeadlines.getEnforcement().orElse(Enforcement.DEFER);
            disarmed = null;
        } else {
            ProvidedDeadline current = contextDeadline.state();
            if (current != null && current.remainingNanos(now) <= timeoutNanos) {
                return context.attach();
            }
            enforcement = current == null ? Enforcement.DEFER : current.enforcement();
            // The new deadline is shortened from the current one, so it is disarmed with it.
            disarmed = current == null ? null : current.disarmed();
        }
        ProvidedDeadline bound = new ProvidedDeadline(timeoutNanos, now, true, enforcement, disarmed);
        return context.with(CONTEXT_DEADLINE, new ContextDeadline(bound)).attach();
    }

    /**
     * Opens a scope in which there is no deadline, regardless of any deadline in an enclosing scope or stored for the
     * current trace. The call context's other values are unchanged, and so is tracing.
     * <p>
     * Inside the scope, {@link #getRemainingDeadline()} and {@link #getEnforcement()} are empty, {@link #checkDeadline}
     * and {@link #awaitWithinDeadline} never throw {@link DeadlineExpiredException}, and {@link #encodeToRequest} sends
     * only the proposed deadline with the client's enforcement (e.g. Dialogue's read timeout). Use this for work
     * that must not be bounded by the current deadline, such as a load shared by several callers or cleanup that must
     * run after the deadline expires.
     * <p>
     * Close the scope on the thread that opened it, which restores the call context that was current before. Work
     * handed off from inside the scope with the call context has no deadline.
     */
    @MustBeClosed
    public static CallContextScope withoutDeadline() {
        return CallContext.current()
                .with(CONTEXT_DEADLINE, ContextDeadline.NONE)
                .attach();
    }

    /**
     * Returns {@code context} with the deadline of an inbound request, without binding it. Bind the result for the
     * duration of the request with {@link CallContext#attach()}.
     * <p>
     * The deadline, its origin and its enforcement are determined as by
     * {@link com.palantir.deadlines.Deadlines#parseFromRequest(Optional, Object, RequestDecodingAdapter, Enforcement)},
     * with the deadline measured from now. If the request has no valid {@link DeadlinesHttpHeaders#EXPECT_WITHIN}
     * header and {@code internalDeadline} is empty, the result has no deadline, so code running with it does not see
     * any deadline stored for the trace.
     *
     * @param context the context to add the deadline to, usually {@link CallContext#empty()} for a new request
     * @param internalDeadline if present, used instead of the request's deadline if it is shorter. A negative value is
     *     treated as zero, and a value longer than {@code Long.MAX_VALUE} ns as {@code Long.MAX_VALUE} ns.
     * @param request the request object to read the deadline from
     * @param adapter reads header values from the request object
     * @param enforcementStrategy configures enforcement strategy (see {@link Enforcement})
     */
    public static <T> CallContext withRequestDeadline(
            CallContext context,
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        ProvidedDeadline parsed = parseState(internalDeadline, request, adapter, enforcementStrategy);
        return context.with(CONTEXT_DEADLINE, parsed == null ? ContextDeadline.NONE : new ContextDeadline(parsed));
    }

    /**
     * Disarms the deadline of the inbound request that {@code context}'s deadline comes from, for example when the
     * response commits, so that work the request started in the background no longer fails on it.
     * <p>
     * The deadline that {@link #withRequestDeadline} parsed for the request, and every deadline that
     * {@link #withDeadline} shortened from it, then no longer applies: wherever it would, there is no deadline, as
     * inside {@link #withoutDeadline()}. This holds on every thread, including for work that captured a context
     * earlier. A deadline that {@link #withDeadline} binds once the request's deadline is disarmed still applies.
     * <p>
     * Disarming cannot be undone. It does nothing if {@code context} has no deadline that comes from a request, and it
     * does not change the deadline stored for the trace.
     */
    public static void disarmRequestDeadline(CallContext context) {
        ContextDeadline contextDeadline = context.get(CONTEXT_DEADLINE);
        ProvidedDeadline deadline = contextDeadline == null ? null : contextDeadline.state();
        AtomicBoolean disarmed = deadline == null ? null : deadline.disarmed();
        if (disarmed != null) {
            disarmed.set(true);
        }
    }

    /**
     * Waits for {@code future} to complete, but no longer than the call context's deadline allows when that
     * deadline is enforced. Unlike the other methods, this ignores the deadline stored for the trace.
     * <p>
     * The call context's deadline is enforced when its {@link Enforcement}, resolved against
     * {@code clientEnforcement} by {@link Enforcement#resolveWith}, is {@link Enforcement#ENFORCE}. In that case:
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
        ContextDeadline contextDeadline = contextDeadline();
        ProvidedDeadline deadline = contextDeadline == null ? null : contextDeadline.state();
        if (future.isDone()
                || deadline == null
                || deadline.enforcement().resolveWith(clientEnforcement) != Enforcement.ENFORCE) {
            return future.get();
        }
        long remainingNanos = deadline.remainingNanos(getClockNanoTime());
        if (remainingNanos > 0) {
            try {
                return future.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                // the deadline expired while waiting; fall through to report it
            }
        }
        if (future.isDone()) {
            // the future completed after the check above, for example as the wait timed out
            return future.get();
        }
        if (deadline.isDisarmed()) {
            // the request was disarmed while waiting, so the deadline no longer applies
            return future.get();
        }
        recordExpiration(deadline.internal(), Expired_Intent.THROW, deadline.valueNanos());
        throw deadline.internal() ? DeadlineExpiredException.internal() : DeadlineExpiredException.external();
    }

    /**
     * Encode a deadline into a request header.
     * <p>
     * The actual deadline value encoded will be the minimum of:
     *   - the providedDeadline parameter
     *   - the value returned by {@link #getRemainingDeadline()} if it exists
     * This ensures that the deadline set for the request will be based on the remaining deadline from
     * already-set internal state, or a smaller one if the caller chooses that.
     * <p>
     * This function does not change the current deadline.
     * <p>
     * The client requested enforcement strategy will be resolved against the internal state in the following manner:
     *   - If either client or internal state requests {@link Enforcement#DISABLE}, then the deadline is not enforced.
     *   - Then, if either client or internal state requests {@link Enforcement#ENFORCE}, then the deadline is enforced.
     *   - Finally, if both the client and internal state requests {@link Enforcement#DEFER}, the deadline is not
     *     enforced and a {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header is not encoded on the new request.
     *
     * @param proposedDeadline a proposed value for the deadline; the actual value used will be the minimum of
     * this value and the time remaining for the current deadline, if there is one. A negative value is treated as
     * zero, and a value longer than {@code Long.MAX_VALUE} nanoseconds (about 292 years) as {@code Long.MAX_VALUE}
     * nanoseconds
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
        ContextDeadline contextDeadline = contextDeadline();
        long proposedDeadlineNanos = timeoutToNanos(proposedDeadline);
        if (contextDeadline == null) {
            TraceDeadlines.encodeToRequest(
                    Duration.ofNanos(proposedDeadlineNanos), request, adapter, clientEnforcement);
            return;
        }
        ProvidedDeadline deadline = contextDeadline.state();
        if (deadline == null) {
            // use proposedDeadline
            checkExpiration(
                    proposedDeadlineNanos,
                    false,
                    false,
                    clientEnforcement == Enforcement.ENFORCE,
                    proposedDeadlineNanos);
            encodeHeaders(request, adapter, proposedDeadlineNanos, clientEnforcement);
            return;
        }
        // use the minimum of proposedDeadline and the current deadline
        long remainingNanos = deadline.remainingNanos(getClockNanoTime());
        Enforcement resolvedEnforcement = deadline.enforcement().resolveWith(clientEnforcement);
        boolean enforced = resolvedEnforcement == Enforcement.ENFORCE;
        if (proposedDeadlineNanos <= remainingNanos) {
            checkExpiration(proposedDeadlineNanos, false, proposedDeadlineNanos <= 0, enforced, deadline.valueNanos());
            encodeHeaders(request, adapter, proposedDeadlineNanos, resolvedEnforcement);
        } else {
            checkExpiration(
                    remainingNanos, deadline.internal(), deadline.alreadyExpired(), enforced, deadline.valueNanos());
            encodeHeaders(request, adapter, remainingNanos, resolvedEnforcement);
        }
    }

    private static <T> void encodeHeaders(
            T request, RequestEncodingAdapter<? super T> adapter, long deadlineNanos, Enforcement enforcement) {
        adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN, durationToHeaderValue(deadlineNanos));
        String enforcedHeaderValue = switch (enforcement) {
            case DISABLE -> "false";
            case ENFORCE -> "true";
            case DEFER -> null;
        };
        if (enforcedHeaderValue != null) {
            adapter.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, enforcedHeaderValue);
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

    // Parses the deadline of an inbound request. Returns null if the request has no valid deadline header and there is
    // no internal deadline.
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
                long internalDeadlineValue = timeoutToNanos(internalDeadline.get());
                if (headerDeadline <= internalDeadlineValue) {
                    return newDeadline(headerDeadline, false, stateEnforcement);
                } else {
                    return newDeadline(internalDeadlineValue, true, stateEnforcement);
                }
            }
        } else if (internalDeadline.isPresent()) {
            // use the deadline provided to this method, which is considered internal
            return newDeadline(timeoutToNanos(internalDeadline.get()), true, stateEnforcement);
        }
        return null;
    }

    // A deadline parsed from a request, with a disarmed flag of its own.
    private static ProvidedDeadline newDeadline(long deadline, boolean internal, Enforcement enforcement) {
        return new ProvidedDeadline(deadline, getClockNanoTime(), internal, enforcement, new AtomicBoolean());
    }

    // The deadline in the current call context, or null if the context does not specify one.
    @Nullable
    private static ContextDeadline contextDeadline() {
        return armed(CallContext.current().get(CONTEXT_DEADLINE));
    }

    // Maps a disarmed deadline to ContextDeadline.NONE: the context still specifies a deadline, and it is none.
    @Nullable
    private static ContextDeadline armed(@Nullable ContextDeadline contextDeadline) {
        ProvidedDeadline deadline = contextDeadline == null ? null : contextDeadline.state();
        return deadline != null && deadline.isDisarmed() ? ContextDeadline.NONE : contextDeadline;
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
            long deadline, boolean internal, boolean alreadyExpired, boolean enforced, long originalBudgetNanos) {
        if (deadline <= 0) {
            // expired
            Expired_Intent intent;
            if (enforced) {
                // intent is always "throw" if enforced = true, regardless of what the other flags are
                intent = Expired_Intent.THROW;
            } else if (alreadyExpired) {
                intent = Expired_Intent.PROPAGATE_ALREADY_EXPIRED;
            } else {
                intent = Expired_Intent.PROPAGATE;
            }

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
                log.warn("Failed to parse 'Expect-Within' header value", UnsafeArg.of("value", value));
            } else if (log.isDebugEnabled()) {
                log.debug("Failed to parse 'Expect-Within' header value", UnsafeArg.of("value", value), exception);
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

    private record ProvidedDeadline(
            long valueNanos,
            long wallClockNanos,
            boolean internal,
            Enforcement enforcement,
            @Nullable AtomicBoolean disarmed) {
        long remainingNanos(long currentWallClockNanos) {
            long elapsed = currentWallClockNanos - this.wallClockNanos;
            return valueNanos - elapsed;
        }

        boolean alreadyExpired() {
            return valueNanos <= 0;
        }

        boolean isDisarmed() {
            return disarmed != null && disarmed.get();
        }
    }

    // The value stored in the call context. A null state means the context has no deadline.
    private record ContextDeadline(@Nullable ProvidedDeadline state) {
        static final ContextDeadline NONE = new ContextDeadline(null);
    }

    interface Clock {
        long nanoTime();
    }
}
