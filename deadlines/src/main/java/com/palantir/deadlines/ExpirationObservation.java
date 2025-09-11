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
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

/**
 * Provides the ability to observe the number of deadline expirations that happen between points in time.
 * <p/>
 * Usage example:
 * <pre>
 *     ExpirationObservation start = ExpirationObservation.start();
 *     // ... operation that may cause deadline expiration ...
 *     ExpirationObservation end = start.observeFrom();
 *     if (end.totalExpirations() > 0) {
 *         // at least one deadline expiration happened
 *     }
 * </pre>
 *
 * Calling {@link #start()} will create a new observation with no recorded expirations.
 *
 * Calling {@link #observeFrom()} will return an observation comparing the current point in time to the count of
 * expirations when the original observation was created.
 *
 * Calling {@link #totalExpirations()} will return the number of deadline expirations that occurred
 * between two observations.
 */
public final class ExpirationObservation {

    private static final DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());

    private final MeterValues start;
    private final MeterValues end;

    private ExpirationObservation(MeterValues start, MeterValues end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Begin a new observation.
     */
    public static ExpirationObservation start() {
        MeterValues now = readMeters();
        return new ExpirationObservation(now, now);
    }

    /**
     * Return a new observation with the number of deadline expirations that have occurred since this observation was
     * initialized.
     *
     * The current observation should have been previously initialized either via a call to this method, or
     * a call to {@link #start()},
     */
    public ExpirationObservation observeFrom() {
        MeterValues now = readMeters();
        return new ExpirationObservation(end, now);
    }

    /**
     * Return the total number of expirations that have occurred since this observation was initialized.
     */
    public long totalExpirations() {
        return nExternalPropagate()
                + nExternalPropagateAlreadyExpired()
                + nExternalIgnore()
                + nInternalPropagate()
                + nInternalPropagateAlreadyExpired()
                + nInternalIgnore();
    }

    @VisibleForTesting
    long totalWithExternalCause() {
        return nExternalPropagate() + nExternalPropagateAlreadyExpired() + nExternalIgnore();
    }

    @VisibleForTesting
    long totalWithInternalCause() {
        return nInternalPropagate() + nInternalPropagateAlreadyExpired() + nInternalIgnore();
    }

    @VisibleForTesting
    long totalWithPropagateIntent() {
        return nExternalPropagate() + nInternalPropagate();
    }

    @VisibleForTesting
    long totalWithPropagateAlreadyExpiredIntent() {
        return nExternalPropagateAlreadyExpired() + nInternalPropagateAlreadyExpired();
    }

    @VisibleForTesting
    long totalWithIgnoreIntent() {
        return nExternalIgnore() + nInternalIgnore();
    }

    @VisibleForTesting
    long nExternalPropagate() {
        return end.externalPropagate - start.externalPropagate;
    }

    @VisibleForTesting
    long nExternalPropagateAlreadyExpired() {
        return end.externalPropagateAlreadyExpired - start.externalPropagateAlreadyExpired;
    }

    @VisibleForTesting
    long nExternalIgnore() {
        return end.externalIgnore - start.externalIgnore;
    }

    @VisibleForTesting
    long nInternalPropagate() {
        return end.internalPropagate - start.internalPropagate;
    }

    @VisibleForTesting
    long nInternalPropagateAlreadyExpired() {
        return end.internalPropagateAlreadyExpired - start.internalPropagateAlreadyExpired;
    }

    @VisibleForTesting
    long nInternalIgnore() {
        return end.internalIgnore - start.internalIgnore;
    }

    private static MeterValues readMeters() {
        long externalPropagate = getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.PROPAGATE);
        long externalPropagateAlreadyExpired =
                getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.PROPAGATE_ALREADY_EXPIRED);
        long externalIgnore = getCountFor(Expired_Cause.EXTERNAL, Expired_Intent.IGNORE);
        long internalPropagate = getCountFor(Expired_Cause.INTERNAL, Expired_Intent.PROPAGATE);
        long internalPropagateAlreadyExpired =
                getCountFor(Expired_Cause.INTERNAL, Expired_Intent.PROPAGATE_ALREADY_EXPIRED);
        long internalIgnore = getCountFor(Expired_Cause.INTERNAL, Expired_Intent.IGNORE);

        return new MeterValues(
                externalPropagate,
                externalPropagateAlreadyExpired,
                externalIgnore,
                internalPropagate,
                internalPropagateAlreadyExpired,
                internalIgnore);
    }

    private static long getCountFor(Expired_Cause cause, Expired_Intent intent) {
        return metrics.expired().cause(cause).intent(intent).build().getCount();
    }

    private record MeterValues(
            long externalPropagate,
            long externalPropagateAlreadyExpired,
            long externalIgnore,
            long internalPropagate,
            long internalPropagateAlreadyExpired,
            long internalIgnore) {}
}
