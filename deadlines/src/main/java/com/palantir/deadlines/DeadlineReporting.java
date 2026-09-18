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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.palantir.deadlines.Deadline.Origin;
import com.palantir.deadlines.DeadlineMetrics.Budget_Origin;
import com.palantir.deadlines.DeadlineMetrics.Expired_Budget;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.deadlines.DeadlineMetrics.Received_Enforcement;
import com.palantir.deadlines.DeadlineMetrics.Received_Origin;
import com.palantir.deadlines.DeadlineMetrics.Received_State;
import com.palantir.deadlines.DeadlineMetrics.Revoked_Reason;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.util.concurrent.TimeUnit;

/**
 * The single home for everything the library records.
 * <p>
 * {@link #received} and {@link #requestCompleted} run once per request that carries a deadline, so the metrics they
 * touch are resolved eagerly rather than rebuilt on each call: going through the generated builder would allocate a
 * builder and a {@link com.palantir.tritium.metrics.registry.MetricName} carrying six tags, then hash that name to
 * look the metric up, on every request.
 */
final class DeadlineReporting {

    @SuppressWarnings("for-rollout:deprecation")
    private static final DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());

    private static final Meter[][][] received = resolveReceived();
    private static final Histogram[] budget = resolveBudget();
    private static final Meter[] revoked = resolveRevoked();
    private static final Histogram headroom = metrics.headroom();
    private static final Meter detached = metrics.detached();

    private DeadlineReporting() {}

    /**
     * Records a deadline being established for a request, and the size of the budget it started with.
     * <p>
     * Indexed by ordinal deliberately: the arrays are built from {@code values()} in this same process, so a
     * reordering of the generated enums cannot give an index a different meaning than it had when the array was
     * filled. The alternative of an {@code EnumMap} returns a nullable value for a map that is total over its key
     * domain, which costs a null check on a per-request path to satisfy a condition that cannot arise.
     */
    @SuppressWarnings("EnumOrdinal")
    static void received(Deadline deadline) {
        Origin origin = deadline.origin();
        received[receivedOrigin(origin).ordinal()][
                receivedEnforcement(deadline.enforcement()).ordinal()][
                receivedState(deadline).ordinal()]
                .mark();
        budget[budgetOrigin(origin).ordinal()].update(toMillis(deadline.budgetNanos()));
    }

    /** Records how much budget was left when the inbound request finished. */
    static void requestCompleted(Deadline deadline) {
        headroom.update(toMillis(Math.max(0L, deadline.remainingNanos())));
    }

    @SuppressWarnings("EnumOrdinal")
    static void revoked(Revoked_Reason reason) {
        revoked[reason.ordinal()].mark();
    }

    static void detached() {
        detached.mark();
    }

    /**
     * Marks the expiration meter. Whether to throw is the caller's decision, so that this stays a recording call
     * rather than hidden control flow.
     */
    static void expired(Origin origin, Expired_Intent intent, long budgetNanos) {
        metrics.expired()
                .cause(origin == Origin.INTERNAL ? Expired_Cause.INTERNAL : Expired_Cause.EXTERNAL)
                .intent(intent)
                .budget(budgetBucket(budgetNanos))
                .build()
                .mark();
    }

    private static Received_Origin receivedOrigin(Origin origin) {
        return switch (origin) {
            case INTERNAL -> Received_Origin.INTERNAL;
            case EXTERNAL -> Received_Origin.EXTERNAL;
        };
    }

    private static Budget_Origin budgetOrigin(Origin origin) {
        return switch (origin) {
            case INTERNAL -> Budget_Origin.INTERNAL;
            case EXTERNAL -> Budget_Origin.EXTERNAL;
        };
    }

    private static Received_Enforcement receivedEnforcement(Enforcement enforcement) {
        return switch (enforcement) {
            case ENFORCE -> Received_Enforcement.ENFORCE;
            case DEFER -> Received_Enforcement.DEFER;
            case DISABLE -> Received_Enforcement.DISABLE;
        };
    }

    private static Received_State receivedState(Deadline deadline) {
        return deadline.expiredOnArrival() ? Received_State.EXPIRED_ON_ARRIVAL : Received_State.LIVE;
    }

    @SuppressWarnings("EnumOrdinal")
    private static Meter[][][] resolveReceived() {
        Meter[][][] meters = new Meter[Received_Origin.values().length][Received_Enforcement.values().length]
                [Received_State.values().length];
        for (Received_Origin origin : Received_Origin.values()) {
            for (Received_Enforcement enforcement : Received_Enforcement.values()) {
                for (Received_State state : Received_State.values()) {
                    meters[origin.ordinal()][enforcement.ordinal()][state.ordinal()] = metrics.received()
                            .origin(origin)
                            .enforcement(enforcement)
                            .state(state)
                            .build();
                }
            }
        }
        return meters;
    }

    @SuppressWarnings("EnumOrdinal")
    private static Histogram[] resolveBudget() {
        Histogram[] histograms = new Histogram[Budget_Origin.values().length];
        for (Budget_Origin origin : Budget_Origin.values()) {
            histograms[origin.ordinal()] = metrics.budget(origin);
        }
        return histograms;
    }

    @SuppressWarnings("EnumOrdinal")
    private static Meter[] resolveRevoked() {
        Meter[] meters = new Meter[Revoked_Reason.values().length];
        for (Revoked_Reason reason : Revoked_Reason.values()) {
            meters[reason.ordinal()] = metrics.revoked(reason);
        }
        return meters;
    }

    private static long toMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static Expired_Budget budgetBucket(long nanos) {
        if (nanos < 100_000_000L) {
            return Expired_Budget.SUB_100MS;
        } else if (nanos < 1_000_000_000L) {
            return Expired_Budget.SUB_1S;
        } else if (nanos < 10_000_000_000L) {
            return Expired_Budget.SUB_10S;
        } else if (nanos < 100_000_000_000L) {
            return Expired_Budget.SUB_100S;
        } else {
            return Expired_Budget.ABOVE_100S;
        }
    }
}
