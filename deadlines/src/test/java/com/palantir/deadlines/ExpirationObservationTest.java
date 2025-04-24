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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import org.junit.jupiter.api.Test;

class ExpirationObservationTest {

    @Test
    public void test_observe_no_changes() {
        ExpirationObservation start = ExpirationObservation.start();
        ExpirationObservation end = start.observeFrom();
        assertThat(end.totalExpirations()).isEqualTo(start.totalExpirations());
    }

    @Test
    public void test_observe_change_external_propagate() {
        ExpirationObservation start = ExpirationObservation.start();

        DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
        metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE)
                .build()
                .mark();

        ExpirationObservation end = start.observeFrom();

        assertThat(end.totalExpirations()).isEqualTo(1);
        assertThat(end.nExternalPropagate()).isEqualTo(1);
    }

    @Test
    public void test_observe_change_multiple_observations() {
        ExpirationObservation start = ExpirationObservation.start();

        DeadlineMetrics metrics = DeadlineMetrics.of(SharedTaggedMetricRegistries.getSingleton());
        metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE)
                .build()
                .mark();

        ExpirationObservation obs1 = start.observeFrom();

        assertThat(obs1.totalExpirations()).isEqualTo(1);
        assertThat(obs1.nExternalPropagate()).isEqualTo(1);

        metrics.expired()
                .cause(Expired_Cause.INTERNAL)
                .intent(Expired_Intent.IGNORE)
                .build()
                .mark();

        ExpirationObservation obs2 = obs1.observeFrom();

        assertThat(obs2.totalExpirations()).isEqualTo(1);
        assertThat(obs2.nInternalIgnore()).isEqualTo(1);
    }
}
