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

import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.Deadlines.Enforcement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A handle to the time budget for a single request.
 * <p>
 * A handle answers every question about the request's budget on its own, from any thread, with no trace attached.
 * That is the point of it: the alternative is to read the remaining time, the enforcement strategy and the origin
 * through separate calls into trace state, which cannot be done at all off-trace and which lets the three answers
 * disagree when read one at a time.
 * <p>
 * Obtain one from {@link Deadlines#current()}. A handle may safely be retained and consulted later, including after
 * the request that created it has finished: {@link #remaining()} recomputes against the clock on each call, so a
 * handle is never a stale snapshot of a duration that a caller might mistake for a fresh budget.
 *
 * <h2>Revocation</h2>
 *
 * A deadline can stop applying before it expires, which is what {@link Deadlines#revoke()} and
 * {@link Deadlines#requestCompleted()} do so that work outliving a finished request is not bound by that request's
 * budget. Revocation is a single write-once flag on this object, visible to every holder of the handle, so a caller
 * that captured a handle earlier does not have to re-read trace state to find out that the deadline no longer
 * applies. The flag only ever moves from unset to set, so a reader sees either "applies" or "does not apply", both of
 * which were true at some instant during the read.
 */
public final class Deadline {

    private static final AtomicIntegerFieldUpdater<Deadline> revokedUpdater =
            AtomicIntegerFieldUpdater.newUpdater(Deadline.class, "revoked");

    private final long budgetNanos;
    private final long startNanos;
    private final Origin origin;
    private final Enforcement enforcement;

    /**
     * Whether this deadline has stopped applying. Written at most once, and only ever from unset to set, so that the
     * caller which performs that transition is the only one that reports it. A field updater rather than an
     * {@link java.util.concurrent.atomic.AtomicBoolean} to keep a handle to one object per request.
     */
    private volatile int revoked;

    private Deadline(long budgetNanos, long startNanos, Origin origin, Enforcement enforcement) {
        this.budgetNanos = budgetNanos;
        this.startNanos = startNanos;
        this.origin = origin;
        this.enforcement = enforcement;
    }

    /**
     * A deadline of the given budget, starting now, imposed by this process and not enforced here.
     * <p>
     * Useful for entry points that are not HTTP requests, for granting a fresh budget to work that must outlive the
     * current request via {@link Deadlines#attach}, and for tests.
     */
    public static Deadline of(Duration budget) {
        return of(budget, Origin.INTERNAL, Enforcement.DEFER);
    }

    /** A deadline of the given budget, starting now. */
    public static Deadline of(Duration budget, Origin origin, Enforcement enforcement) {
        return ofNanos(budget.toNanos(), origin, enforcement);
    }

    static Deadline ofNanos(long budgetNanos, Origin origin, Enforcement enforcement) {
        return new Deadline(budgetNanos, Deadlines.clockNanoTime(), origin, enforcement);
    }

    /**
     * The time left before this deadline, or {@link Duration#ZERO} if it has passed.
     * <p>
     * Recomputed on each call. Prefer {@link #isExpired()} over comparing this to {@link Duration#ZERO}, and note
     * that a revoked deadline still reports the time left by this arithmetic even though it no longer applies.
     */
    public Duration remaining() {
        return Duration.ofNanos(Math.max(0L, remainingNanos()));
    }

    /**
     * Whether this deadline has passed and still applies.
     * <p>
     * This is the question almost every caller wants: a revoked deadline reports false however long ago its budget
     * ran out, because it no longer constrains anything. Use {@link #hasPassed()} to ask about the budget alone.
     */
    public boolean isExpired() {
        return !isRevoked() && hasPassed();
    }

    /**
     * Whether this deadline's budget has run out, whether or not it still applies.
     * <p>
     * For reporting on a request after the fact, where a deadline that ran out and was then revoked is still worth
     * recording. Callers deciding whether to stop work want {@link #isExpired()}.
     */
    public boolean hasPassed() {
        return remainingNanos() <= 0;
    }

    /** Whether this deadline has stopped applying. See {@link Deadlines#revoke()}. */
    public boolean isRevoked() {
        return revoked != 0;
    }

    /** Which value became the deadline for this request. */
    public Origin origin() {
        return origin;
    }

    /** The enforcement strategy established for this request. */
    public Enforcement enforcement() {
        return enforcement;
    }

    /**
     * Whether this deadline is enforced against a caller requesting {@code clientEnforcement}.
     * <p>
     * This is the one place the two enforcement strategies are resolved against each other, so that callers do not
     * have to rediscover that {@link Enforcement#DISABLE} wins over {@link Enforcement#ENFORCE} and that
     * {@link Enforcement#DEFER} yields to whatever the other side asked for. Comparing {@link #enforcement()} to
     * {@link Enforcement#ENFORCE} directly is not equivalent, and misses the common case of a deferring server
     * called through an enforcing client.
     */
    public boolean bindsUnder(Enforcement clientEnforcement) {
        return !isRevoked() && enforcement.resolveWith(clientEnforcement) == Enforcement.ENFORCE;
    }

    /**
     * Fails if this deadline has expired and is enforced against a caller requesting {@code clientEnforcement}.
     * <p>
     * For callers that are not sending a request, such as work waiting on a budget bounded by the deadline. A no-op
     * when the deadline has not expired or has been revoked. An expiration that is not enforced is recorded and
     * ignored, leaving the caller to decide whether to continue.
     *
     * @throws DeadlineExpiredException if this deadline has expired and is enforced
     */
    public void check(Enforcement clientEnforcement) {
        if (isRevoked() || !hasPassed()) {
            return;
        }
        boolean enforced = enforcement.resolveWith(clientEnforcement) == Enforcement.ENFORCE;
        DeadlineReporting.expired(origin, enforced ? Expired_Intent.THROW : Expired_Intent.IGNORE, budgetNanos);
        if (enforced) {
            throw newException();
        }
    }

    /**
     * Records this deadline's expiration and returns the exception to fail the request with.
     * <p>
     * The counterpart to {@link #check} for callers that complete a future rather than throw, such as a queue giving
     * up on a request whose budget has run out. Recording here rather than in the caller keeps every expiration that
     * fails a request in the same meter, regardless of how the caller reports the failure.
     */
    public DeadlineExpiredException expire() {
        DeadlineReporting.expired(origin, Expired_Intent.THROW, budgetNanos);
        return newException();
    }

    @Override
    public String toString() {
        return "Deadline{budget=" + Duration.ofNanos(budgetNanos)
                + ", remaining=" + remaining()
                + ", origin=" + origin
                + ", enforcement=" + enforcement
                + (isRevoked() ? ", revoked" : "")
                + '}';
    }

    DeadlineExpiredException newException() {
        return DeadlineExpiredException.of(origin, budgetNanos, elapsedNanos());
    }

    /** Stops this deadline applying, returning whether this call was the one that did it. */
    boolean revoke() {
        return revokedUpdater.compareAndSet(this, 0, 1);
    }

    long remainingNanos() {
        return budgetNanos - elapsedNanos();
    }

    long elapsedNanos() {
        return Deadlines.clockNanoTime() - startNanos;
    }

    long budgetNanos() {
        return budgetNanos;
    }

    /**
     * Whether this deadline had already run out when it was established, meaning an upstream node propagated an
     * expired deadline rather than failing the request itself.
     */
    boolean expiredOnArrival() {
        return budgetNanos <= 0;
    }

    /** Whichever of the two deadlines runs out first, carrying that deadline's origin with it. */
    static Deadline earlier(Deadline first, Deadline second) {
        return first.remainingNanos() <= second.remainingNanos() ? first : second;
    }

    /** Identifies which value became the deadline for a request. */
    public enum Origin {
        /**
         * The deadline was imposed by this process, either because none was received from a caller or because this
         * process' own budget was the smaller of the two.
         */
        INTERNAL,

        /** The deadline was received from a caller. */
        EXTERNAL
    }
}
