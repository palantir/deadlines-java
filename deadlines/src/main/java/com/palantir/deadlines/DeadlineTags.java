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

import com.palantir.deadlines.Deadline.Origin;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.tracing.TagTranslator;
import java.util.concurrent.TimeUnit;

/**
 * Describes a {@link Deadline} as span tags, so that a trace shows where a request's budget went.
 * <p>
 * Metrics say how often deadlines expire but not which hop consumed the budget; that question needs the per-request
 * detail a trace carries. This library deliberately does not start spans of its own to hold these tags, because a
 * span's tags can only be set when it starts or completes, so tagging from in here would mean an extra span on every
 * request. Instead, apply this where a span you already own starts or completes:
 *
 * <pre>{@code
 * Deadlines.current().ifPresent(deadline -> span.complete(DeadlineTags.INSTANCE, deadline));
 * }</pre>
 *
 * A singleton, so applying it costs no allocation.
 */
public enum DeadlineTags implements TagTranslator<Deadline> {
    INSTANCE;

    @Override
    public <T> void translate(TagAdapter<T> adapter, T target, Deadline deadline) {
        adapter.tag(target, "deadline.budgetMillis", millis(deadline.budgetNanos()));
        adapter.tag(target, "deadline.remainingMillis", millis(deadline.remainingNanos()));
        adapter.tag(target, "deadline.origin", tagValue(deadline.origin()));
        adapter.tag(target, "deadline.enforcement", tagValue(deadline.enforcement()));
        if (deadline.isRevoked()) {
            adapter.tag(target, "deadline.revoked", "true");
        }
    }

    private static String millis(long nanos) {
        return Long.toString(TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    private static String tagValue(Origin origin) {
        return switch (origin) {
            case INTERNAL -> "internal";
            case EXTERNAL -> "external";
        };
    }

    private static String tagValue(Enforcement enforcement) {
        return switch (enforcement) {
            case ENFORCE -> "enforce";
            case DEFER -> "defer";
            case DISABLE -> "disable";
        };
    }
}
