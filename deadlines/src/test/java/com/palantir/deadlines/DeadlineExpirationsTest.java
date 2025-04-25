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

import com.palantir.deadlines.DeadlineExpirations.DeadlineExpirationValues;
import com.palantir.deadlines.DeadlineMetrics.Expired_Cause;
import com.palantir.deadlines.DeadlineMetrics.Expired_Intent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class DeadlineExpirationsTest {
    DeadlineMetrics metrics;

    @BeforeEach
    public void before() {
        metrics = DeadlineMetrics.of(new DefaultTaggedMetricRegistry());
    }

    @Test
    public void test_observe_no_changes() {
        DeadlineExpirations deadlineExpirations = new DeadlineExpirations(metrics);
        DeadlineExpirationValues start = deadlineExpirations.get();
        DeadlineExpirationValues obs = deadlineExpirations.get().minus(start);
        assertThat(obs.totalExpirations()).isEqualTo(start.totalExpirations());
    }

    @Test
    public void test_observe_change_external_propagate() {
        DeadlineExpirations deadlineExpirations = new DeadlineExpirations(metrics);
        DeadlineExpirationValues start = deadlineExpirations.get();

        metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE)
                .build()
                .mark();

        DeadlineExpirationValues obs = deadlineExpirations.get().minus(start);

        assertThat(obs.totalExpirations()).isEqualTo(1);
        assertThat(obs.externalExpirations().propagate()).isEqualTo(1);
    }

    @Test
    public void test_observe_change_multiple_observations() {
        DeadlineExpirations deadlineExpirations = new DeadlineExpirations(metrics);
        DeadlineExpirationValues start = deadlineExpirations.get();

        metrics.expired()
                .cause(Expired_Cause.EXTERNAL)
                .intent(Expired_Intent.PROPAGATE)
                .build()
                .mark();

        DeadlineExpirationValues obs1 = deadlineExpirations.get().minus(start);

        assertThat(obs1.totalExpirations()).isEqualTo(1);
        assertThat(obs1.externalExpirations().propagate()).isEqualTo(1);

        metrics.expired()
                .cause(Expired_Cause.INTERNAL)
                .intent(Expired_Intent.IGNORE)
                .build()
                .mark();

        DeadlineExpirationValues obs2 = deadlineExpirations.get().minus(obs1);

        assertThat(obs2.totalExpirations()).isEqualTo(1);
        assertThat(obs2.internalExpirations().ignore()).isEqualTo(1);
    }
}