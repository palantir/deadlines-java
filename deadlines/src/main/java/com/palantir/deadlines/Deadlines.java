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
import com.google.errorprone.annotations.InlineMe;
import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.deadlines.Deadline.Origin;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.DeadlineMetrics.Revoked_Reason;
import com.palantir.tracing.TraceLocal;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Establishes, propagates and enforces the time budget for a request.
 * <p>
 * A server calls {@link #fromRequest} once per inbound request to establish the budget, and
 * {@link #requestCompleted()} once when that request finishes. A client calls {@link #toRequest} to pass the
 * remaining budget to the next hop. Everything in between reads the budget through {@link #current()}, which returns
 * a {@link Deadline} handle answering every question about it.
 * <p>
 * The budget is held in trace state, so it is shared by every thread participating in a trace without being passed
 * explicitly. {@link #fromRequest} is the only method that writes it; {@link #attach} and {@link #detach()} layer a
 * scoped override on top, and neither changes the deadline the request started with.
 * <p>
 * Deadline expiration does not interrupt work in progress. It prevents new outbound calls, so an expiration is only
 * ever noticed at the next call to {@link #toRequest} or {@link Deadline#check}.
 */
public final class Deadlines {

    private Deadlines() {}

    private static final TraceLocal<Deadline> deadlineState = TraceLocal.of();

    /**
     * Threads which should not observe the deadline stored for the trace they are running in. This is deliberately
     * not trace state: a trace's TraceLocal values are shared by reference with every other thread participating in
     * that trace, so hiding a deadline by mutating them would hide it from the whole trace.
     */
    private static final ThreadLocal<Boolean> detached = new ThreadLocal<>();

    private static volatile Clock clock = System::nanoTime;

    /**
     * The deadline established for the current request, unless it is hidden on this thread by {@link #detach()}.
     * <p>
     * Returns a handle rather than a duration so that the remaining time, the enforcement strategy and the origin are
     * read once and cannot disagree, and so that the answer stays usable on a thread with no trace attached.
     * <p>
     * A revoked deadline is still returned; ask the handle. This differs from the deprecated
     * {@link #getRemainingDeadline()}, which cannot tell "no deadline" from "revoked" from "expired".
     */
    public static Optional<Deadline> current() {
        return Optional.ofNullable(visible());
    }

    /**
     * Establishes the deadline for an inbound request, and returns it.
     * <p>
     * The budget is the smaller of the {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header on the request and
     * {@code serverBudget}, with the received value winning ties so that a server's own budget can only ever shorten
     * a request's time, never extend it. {@link Deadline#origin()} records which one was chosen.
     * <p>
     * Enforcement is {@code enforcement} resolved against the request's
     * {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} header, which lets a caller sending
     * {@code Expect-Within-Enforced: false} switch enforcement off for this node and everything downstream of it.
     *
     * @param request the inbound request to read headers from
     * @param reader reads a header from {@code request}
     * @param serverBudget a budget this server imposes on itself, used when it is smaller than the received one
     * @param enforcement this server's enforcement strategy
     * @return the established deadline, or empty when neither a header nor a {@code serverBudget} was present
     */
    public static <T> Optional<Deadline> fromRequest(
            T request, HeaderReader<? super T> reader, Optional<Duration> serverBudget, Enforcement enforcement) {
        Long headerNanos = ExpectWithinHeader.parse(reader.firstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN));
        String headerEnforced = reader.firstHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED);
        Enforcement resolved =
                enforcementFromHeader(headerEnforced, headerNanos != null).resolveWith(enforcement);

        Deadline deadline = select(headerNanos, serverBudget, resolved);
        if (deadline == null) {
            return Optional.empty();
        }
        deadlineState.set(deadline);
        DeadlineReporting.received(deadline);
        return Optional.of(deadline);
    }

    @Nullable
    private static Deadline select(
            @Nullable Long headerNanos, Optional<Duration> serverBudget, Enforcement enforcement) {
        if (headerNanos == null) {
            return serverBudget.isEmpty()
                    ? null
                    : Deadline.ofNanos(serverBudget.get().toNanos(), Origin.INTERNAL, enforcement);
        }
        if (serverBudget.isEmpty()) {
            return Deadline.ofNanos(headerNanos, Origin.EXTERNAL, enforcement);
        }
        // The received deadline wins ties: this server's own budget may only shorten a request, never extend it.
        long serverNanos = serverBudget.get().toNanos();
        return headerNanos <= serverNanos
                ? Deadline.ofNanos(headerNanos, Origin.EXTERNAL, enforcement)
                : Deadline.ofNanos(serverNanos, Origin.INTERNAL, enforcement);
    }

    /**
     * Encodes the remaining budget onto an outbound request, failing first if it has already run out.
     * <p>
     * The value encoded is the smaller of {@code proposed} and the time left on the current deadline, so a caller can
     * ask for less than it has but never for more. The enforcement strategy encoded is the current deadline's,
     * resolved against {@code clientEnforcement}.
     * <p>
     * Writes nothing when the current deadline has been revoked, because a revoked deadline must not bind the next
     * hop either.
     *
     * @param request the outbound request to write headers to
     * @param writer writes a header onto {@code request}
     * @param proposed the largest budget this caller is willing to grant
     * @param clientEnforcement this caller's enforcement strategy
     * @throws DeadlineExpiredException if the budget has run out and enforcement resolves to
     *     {@link Enforcement#ENFORCE}
     */
    public static <T> void toRequest(
            T request, HeaderWriter<? super T> writer, Duration proposed, Enforcement clientEnforcement) {
        // Read trace state exactly once: every fact below has to describe the same deadline.
        Deadline current = visible();
        long proposedNanos = proposed.toNanos();

        if (current == null) {
            // Only this process' own proposal constrains the request, so an expiration of it is internally caused.
            reportIfExpired(
                    proposedNanos,
                    Origin.INTERNAL,
                    proposedNanos,
                    0L,
                    expiryIntent(clientEnforcement == Enforcement.ENFORCE, false, false));
            encode(request, writer, proposedNanos, clientEnforcement);
            return;
        }

        Enforcement resolved = current.enforcement().resolveWith(clientEnforcement);
        boolean revoked = current.isRevoked();
        long remainingNanos = current.remainingNanos();

        // Whichever budget is smaller binds, and its origin travels with it. Deriving the origin here rather than
        // passing a flag is what keeps an exhausted server-side budget from being reported as a caller's fault.
        boolean proposedBinds = proposedNanos <= remainingNanos;
        long bindingNanos = proposedBinds ? proposedNanos : remainingNanos;
        Origin origin = proposedBinds ? Origin.INTERNAL : current.origin();

        reportIfExpired(
                bindingNanos,
                origin,
                current.budgetNanos(),
                current.elapsedNanos(),
                // A revoked deadline no longer constrains anything, so it must not decide enforcement either.
                expiryIntent(!revoked && resolved == Enforcement.ENFORCE, revoked, current.expiredOnArrival()));

        if (!revoked) {
            encode(request, writer, bindingNanos, resolved);
        }
    }

    /**
     * Hides the current deadline from the current thread until the returned scope is closed.
     * <p>
     * Within the scope {@link #current()} is empty and {@link #toRequest} behaves as though no deadline had been
     * established: {@code proposed} is encoded as-is rather than reduced, and an exhausted budget is not enforced.
     * <p>
     * This exists for work that is shared between callers rather than performed on behalf of one of them, such as a
     * cache load which several requests are waiting on. Such work inherits the trace, and therefore the deadline, of
     * whichever caller happened to start it; without detaching, the caller with the smallest budget can fail work
     * that every other waiter still has ample time for.
     * <p>
     * Applies only to the calling thread. It does not change the deadline established for the request, so threads
     * running concurrently in the same trace are unaffected, and it is not inherited by work handed off to another
     * thread from inside the scope. Use {@link #attach} when the override should cover such work.
     */
    @MustBeClosed
    public static CloseableDeadlineScope detach() {
        if (Boolean.TRUE.equals(detached.get())) {
            return DeadlineScope.ALREADY_DETACHED;
        }
        DeadlineReporting.detached();
        detached.set(Boolean.TRUE);
        return DeadlineScope.RESTORE_ON_CLOSE;
    }

    /**
     * Replaces the current request's deadline until the returned scope is closed.
     * <p>
     * For work that must outlive the budget of the request that started it, such as writing that request's own
     * outcome once it has given up: granting a fresh budget with {@code attach(Deadline.of(Duration.ofMinutes(1)))}
     * lets those calls through without abandoning deadlines for the rest of the request. Also the supported way to
     * establish a deadline outside an inbound HTTP request, including in tests.
     * <p>
     * Unlike {@link #detach()} this covers every thread in the trace, so work handed off from inside the scope
     * inherits the replacement. That is usually what such work needs, and it is the reason to prefer this over
     * {@link #revoke()}, which cannot be undone.
     */
    @MustBeClosed
    public static CloseableDeadlineScope attach(Deadline deadline) {
        DeadlineReporting.detached();
        Deadline previous = deadlineState.set(deadline);
        return previous == null ? () -> deadlineState.remove() : () -> deadlineState.set(previous);
    }

    /**
     * Stops the current deadline applying, for good.
     * <p>
     * Every holder of the {@link Deadline} handle sees this, including callers on other threads which captured it
     * earlier, so work already waiting on the budget finds out that it no longer needs to.
     * <p>
     * Prefer {@link #attach} where the work that must escape the deadline is bounded, since this cannot be undone and
     * leaves the rest of the request unbounded. Servers should call {@link #requestCompleted()} instead, which
     * revokes and records how much budget was left.
     */
    public static void revoke() {
        revoke(Revoked_Reason.CALLER);
    }

    private static void revoke(Revoked_Reason reason) {
        Deadline deadline = deadlineState.get();
        if (deadline != null && deadline.revoke()) {
            DeadlineReporting.revoked(reason);
        }
    }

    /**
     * Records how much budget was left and stops the deadline applying. Call once when an inbound request finishes.
     * <p>
     * Two things happen here because they belong to the same moment. The remaining budget is worth recording only at
     * the point the request ends, and from that point work outliving the request — a background task, a final write
     * of the request's own outcome — is no longer being performed on the caller's time and should not be bound by the
     * caller's budget.
     *
     * @return the deadline that was completed, for callers that want to report on it. {@link Deadline#hasPassed()}
     *     answers whether the request overran and keeps answering it after this call, whereas
     *     {@link Deadline#isExpired()} does not, because the deadline no longer applies.
     */
    public static Optional<Deadline> requestCompleted() {
        Deadline deadline = deadlineState.get();
        if (deadline == null) {
            return Optional.empty();
        }
        DeadlineReporting.requestCompleted(deadline);
        if (deadline.revoke()) {
            DeadlineReporting.revoked(Revoked_Reason.REQUEST_COMPLETED);
        }
        return Optional.of(deadline);
    }

    /**
     * The deadline stored for the current trace, unless it is hidden on this thread.
     * <p>
     * The null check is first so that traces without a deadline, which is the common case for services that do not
     * receive one, do not pay for the additional thread local read.
     */
    @Nullable
    private static Deadline visible() {
        Deadline deadline = deadlineState.get();
        if (deadline == null || Boolean.TRUE.equals(detached.get())) {
            return null;
        }
        return deadline;
    }

    private static <T> void encode(
            T request, HeaderWriter<? super T> writer, long deadlineNanos, Enforcement enforcement) {
        writer.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN, ExpectWithinHeader.format(deadlineNanos));
        String enforced =
                switch (enforcement) {
                    case DISABLE -> "false";
                    case ENFORCE -> "true";
                    case DEFER -> null;
                };
        if (enforced != null) {
            writer.setHeader(request, DeadlinesHttpHeaders.EXPECT_WITHIN_ENFORCED, enforced);
        }
    }

    private static void reportIfExpired(
            long bindingNanos, Origin origin, long budgetNanos, long elapsedNanos, Expired_Intent intent) {
        if (bindingNanos > 0) {
            return;
        }
        DeadlineReporting.expired(origin, intent, budgetNanos);
        if (intent == Expired_Intent.THROW) {
            throw DeadlineExpiredException.of(origin, budgetNanos, elapsedNanos);
        }
    }

    /**
     * Classifies what is about to happen to an expired deadline, for the {@code deadline.expired} meter.
     * <p>
     * The three inputs are independent: enforcement decides whether the request fails here, revocation decides
     * whether the expiration is carried onward, and arrival state distinguishes an expiration this node caused from
     * one it merely inherited.
     */
    private static Expired_Intent expiryIntent(boolean enforced, boolean revoked, boolean expiredOnArrival) {
        if (enforced) {
            return Expired_Intent.THROW;
        }
        if (revoked) {
            return Expired_Intent.IGNORE;
        }
        return expiredOnArrival ? Expired_Intent.PROPAGATE_ALREADY_EXPIRED : Expired_Intent.PROPAGATE;
    }

    private static Enforcement enforcementFromHeader(@Nullable String headerEnforced, boolean headerDeadlineSet) {
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

    private enum DeadlineScope implements CloseableDeadlineScope {
        RESTORE_ON_CLOSE {
            @Override
            public void close() {
                detached.remove();
            }
        },
        ALREADY_DETACHED {
            @Override
            public void close() {}
        }
    }

    /**
     * Selects whether an expired deadline fails a request.
     * <p>
     * The three strategies form a lattice: {@link #DISABLE} wins over everything, {@link #ENFORCE} wins over
     * {@link #DEFER}, and {@link #DEFER} yields to whatever the other side asked for. Callers should not resolve
     * strategies by hand; {@link Deadline#bindsUnder} and {@link Deadline#check} apply this for them.
     */
    public enum Enforcement {
        /**
         * Fail the request when the deadline has expired, and ask downstream nodes to do the same by sending
         * {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} set to "true".
         * <p>
         * A caller sending that header set to "false" still overrides this, so that an upstream node can
         * short-circuit enforcement for a request it knows must complete.
         */
        ENFORCE,

        /**
         * Do not fail the request here unless a caller asked for enforcement by sending
         * {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} set to "true", and do not ask downstream nodes to.
         * <p>
         * The default, and the safest strategy for initial adoption: deadlines are tracked and propagated but
         * expiration changes nothing until someone opts in.
         */
        DEFER,

        /**
         * Do not fail the request here or at any downstream node, whatever they are configured to do, by sending
         * {@link DeadlinesHttpHeaders#EXPECT_WITHIN_ENFORCED} set to "false".
         * <p>
         * This terminates enforcement for the rest of the request chain, which is useful for protecting a workflow
         * that must always run to completion.
         */
        DISABLE;

        /**
         * Resolves this strategy against another, producing the strategy that applies when both have an opinion.
         * <p>
         * Commutative and associative, with {@link #DEFER} as the identity and {@link #DISABLE} absorbing. Note that
         * {@link #DEFER} being the identity means it does not suppress enforcement requested by the other side.
         */
        public Enforcement resolveWith(Enforcement other) {
            return switch (this) {
                case DISABLE -> this;
                case ENFORCE -> other == Enforcement.DISABLE ? other : this;
                case DEFER -> other;
            };
        }
    }

    /**
     * Get the amount of time remaining for the current deadline.
     *
     * @deprecated Use {@link #current()} and {@link Deadline#remaining()}. This method reports an absent deadline, a
     *     revoked deadline and an expired deadline in ways that cannot be told apart: absent and revoked both return
     *     empty, and expired returns {@link Duration#ZERO}.
     */
    @Deprecated
    public static Optional<Duration> getRemainingDeadline() {
        Deadline deadline = visible();
        if (deadline == null || deadline.isRevoked()) {
            return Optional.empty();
        }
        return Optional.of(deadline.remaining());
    }

    /**
     * Get the enforcement strategy for the current deadline.
     *
     * @deprecated Use {@link #current()} with {@link Deadline#bindsUnder} or {@link Deadline#check}, which resolve
     *     this strategy against the caller's own. Comparing the returned value to {@link Enforcement#ENFORCE}
     *     directly misses a deferring server called through an enforcing client.
     */
    @Deprecated
    public static Optional<Enforcement> getEnforcement() {
        Deadline deadline = visible();
        if (deadline == null) {
            return Optional.empty();
        }
        // A revoked deadline reported DEFER before revocation became a flag of its own. Preserved so that callers
        // comparing this against ENFORCE keep treating a revoked deadline as not enforced.
        return Optional.of(deadline.isRevoked() ? Enforcement.DEFER : deadline.enforcement());
    }

    /**
     * Enforce the current deadline, if it has expired and enforcement is enabled.
     *
     * @deprecated Use {@link #current()} and {@link Deadline#check}.
     */
    @Deprecated
    public static void checkDeadline(Enforcement clientEnforcement) {
        Deadline deadline = visible();
        if (deadline != null) {
            deadline.check(clientEnforcement);
        }
    }

    /**
     * Hides the current trace's deadline from the current thread until the returned scope is closed.
     *
     * @deprecated Use {@link #detach()}.
     */
    @Deprecated
    @MustBeClosed
    public static CloseableDeadlineScope suppressDeadline() {
        return detach();
    }

    /**
     * Disables propagation of deadline values any further for the current trace.
     *
     * @deprecated Use {@link #requestCompleted()} when an inbound request finishes, {@link #attach} to grant a
     *     bounded budget to work that must outlive it, or {@link #revoke()} for the same unbounded behaviour.
     */
    @Deprecated
    @InlineMe(replacement = "Deadlines.revoke()", imports = "com.palantir.deadlines.Deadlines")
    public static void disableFurtherDeadlinePropagation() {
        revoke();
    }

    /**
     * Encode a deadline into a request header.
     *
     * @deprecated Use {@link #toRequest}, which takes the request first and a {@link HeaderWriter}.
     */
    @Deprecated
    @InlineMe(
            replacement = "Deadlines.toRequest(request, adapter, proposedDeadline, clientEnforcement)",
            imports = "com.palantir.deadlines.Deadlines")
    public static <T> void encodeToRequest(
            Duration proposedDeadline,
            T request,
            RequestEncodingAdapter<? super T> adapter,
            Enforcement clientEnforcement) {
        toRequest(request, adapter, proposedDeadline, clientEnforcement);
    }

    /**
     * Encode a deadline into a request header.
     *
     * @deprecated Use {@link #toRequest}.
     */
    @Deprecated
    @InlineMe(
            replacement = "Deadlines.toRequest(request, adapter, proposedDeadline, Enforcement.DEFER)",
            imports = {"com.palantir.deadlines.Deadlines", "com.palantir.deadlines.Deadlines.Enforcement"})
    public static <T> void encodeToRequest(
            Duration proposedDeadline, T request, RequestEncodingAdapter<? super T> adapter) {
        toRequest(request, adapter, proposedDeadline, Enforcement.DEFER);
    }

    /**
     * Parse a deadline value from a request header and set the deadline state for the current trace.
     *
     * @deprecated Use {@link #fromRequest}, which takes the request first, takes a {@link HeaderReader}, and returns
     *     the deadline it established rather than nothing.
     */
    @Deprecated
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        fromRequest(request, adapter, internalDeadline, enforcementStrategy);
    }

    /**
     * Parse a deadline value from a request header and set the deadline state for the current trace.
     *
     * @deprecated Use {@link #fromRequest}.
     */
    @Deprecated
    public static <T> void parseFromRequest(
            Optional<Duration> internalDeadline, T request, RequestDecodingAdapter<? super T> adapter) {
        // by default use DEFER, which matches behavior of consumers on older versions which have no enforcement
        fromRequest(request, adapter, internalDeadline, Enforcement.DEFER);
    }

    /**
     * @deprecated Implement {@link HeaderWriter} instead.
     */
    @Deprecated
    public interface RequestEncodingAdapter<REQUEST> extends HeaderWriter<REQUEST> {}

    /**
     * @deprecated Implement {@link HeaderReader} instead.
     */
    @Deprecated
    public interface RequestDecodingAdapter<REQUEST> extends HeaderReader<REQUEST> {
        @Deprecated
        Optional<String> getFirstHeader(REQUEST request, String headerName);

        @Deprecated
        @Nullable
        default String maybeFirstHeader(REQUEST request, String headerName) {
            return getFirstHeader(request, headerName).orElse(null);
        }

        @Override
        @Nullable
        default String firstHeader(REQUEST request, String headerName) {
            return maybeFirstHeader(request, headerName);
        }
    }

    static long clockNanoTime() {
        return clock.nanoTime();
    }

    @VisibleForTesting
    static void setClock(Clock newClock) {
        clock = newClock;
    }

    /**
     * Restores the real clock. Tests must call this so that a frozen clock does not leak into whichever test runs
     * next.
     */
    @VisibleForTesting
    static void resetClock() {
        clock = System::nanoTime;
    }

    interface Clock {
        long nanoTime();
    }
}
