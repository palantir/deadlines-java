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

import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import org.jetbrains.annotations.VisibleForTesting;

public final class DeadlineExpirations {
    private final DeadlineMetrics metrics;

    @VisibleForTesting
    DeadlineExpirations(DeadlineMetrics metrics) {
        this.metrics = metrics;
    }

    public DeadlineExpirations() {
        this.metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
    }

    public DeadlineExpirationValues get() {
        long externalPropagate = getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.PROPAGATE);
        long externalPropagateAlreadyExpired =
                getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.PROPAGATE_ALREADY_EXPIRED);
        long externalIgnore = getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.IGNORE);
        long internalPropagate = getCountFor(Expired_Cause.INTERNAL, Expired_Intent.PROPAGATE);
        long internalPropagateAlreadyExpired =
                getCountFor(Expired_Cause.INTERNAL, Expired_Intent.PROPAGATE_ALREADY_EXPIRED);
        long internalIgnore = getCountFor(Expired_Cause.INTERNAL, Expired_Intent.IGNORE);

        return new DeadlineExpirationValues(
                new DeadlineExpirationValuesByIntent(
                        externalPropagate, externalPropagateAlreadyExpired, externalIgnore),
                new DeadlineExpirationValuesByIntent(
                        internalPropagate, internalPropagateAlreadyExpired, internalIgnore));
    }

    private long getCountFor(Expired_Cause cause, Expired_Intent intent) {
        return metrics.expired().cause(cause).intent(intent).build().getCount();
    }

    public record DeadlineExpirationValues(
            DeadlineExpirationValuesByIntent externalExpirations,
            DeadlineExpirationValuesByIntent internalExpirations) {

        public DeadlineExpirationValues minus(DeadlineExpirationValues other) {
            return new DeadlineExpirationValues(
                    externalExpirations.minus(other.externalExpirations),
                    internalExpirations.minus(other.internalExpirations));
        }

        public long totalExpirations() {
            return externalExpirations.totalExpirations() + internalExpirations.totalExpirations();
        }
    }

    public record DeadlineExpirationValuesByIntent(long propagate, long propagateAlreadyExpired, long ignore) {
        DeadlineExpirationValuesByIntent minus(DeadlineExpirationValuesByIntent other) {
            return new DeadlineExpirationValuesByIntent(
                    propagate - other.propagate,
                    propagateAlreadyExpired - other.propagateAlreadyExpired,
                    ignore - other.ignore);
        }

        public long totalExpirations() {
            return propagate + propagateAlreadyExpired + ignore;
        }
    }
}
