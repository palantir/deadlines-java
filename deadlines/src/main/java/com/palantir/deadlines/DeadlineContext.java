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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.deadlines.Deadlines.Enforcement;
import com.palantir.deadlines.Deadlines.ProvidedDeadline;
import com.palantir.deadlines.Deadlines.RequestDecodingAdapter;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * An immutable deadline context: either no deadline, or a deadline with a fixed expiry, an origin (internal to this
 * service, or external from a request header) and an {@link Enforcement} strategy.
 * <p>
 * Capture the context that is current on this thread with {@link #current()}, and bind a context on any thread with
 * {@link #attach()}. A context never changes after it is created; only time passes, so the time remaining until its
 * deadline decreases. Contexts have identity semantics.
 */
public final class DeadlineContext {

    private static final DeadlineContext NO_DEADLINE = new DeadlineContext(null);

    @Nullable
    private final ProvidedDeadline state;

    private DeadlineContext(@Nullable ProvidedDeadline state) {
        this.state = state;
    }

    /**
     * Returns the context that is current on this thread: the context bound by the innermost open
     * {@link DeadlineScope}; otherwise the deadline stored for the current trace by the deprecated
     * {@link Deadlines#parseFromRequest(Optional, Object, RequestDecodingAdapter, Enforcement)}; otherwise no deadline.
     */
    public static DeadlineContext current() {
        return of(Deadlines.currentState());
    }

    /**
     * Creates a context from a request's deadline headers, without binding it.
     * <p>
     * The deadline, its origin and its enforcement are determined exactly as by
     * {@link Deadlines#parseFromRequest(Optional, Object, RequestDecodingAdapter, Enforcement)}, with the deadline
     * measured from now. The result has no deadline if the request has no valid
     * {@link DeadlinesHttpHeaders#EXPECT_WITHIN} header and {@code internalDeadline} is empty.
     *
     * @param internalDeadline if present, used instead of the request's deadline if it is shorter
     * @param request the request object to read the deadline from
     * @param adapter reads header values from the request object
     * @param enforcementStrategy configures enforcement strategy (see {@link Enforcement})
     */
    public static <T> DeadlineContext fromRequest(
            Optional<Duration> internalDeadline,
            T request,
            RequestDecodingAdapter<? super T> adapter,
            Enforcement enforcementStrategy) {
        return of(Deadlines.parseState(internalDeadline, request, adapter, enforcementStrategy));
    }

    /**
     * Opens a scope that binds this context on the current thread until the scope is closed. Close the scope on this
     * thread, which restores the context that was current before. Never throws, even if the deadline has expired.
     */
    @MustBeClosed
    public DeadlineScope attach() {
        return DeadlineScope.open(state);
    }

    private static DeadlineContext of(@Nullable ProvidedDeadline state) {
        return state == null ? NO_DEADLINE : new DeadlineContext(state);
    }
}
