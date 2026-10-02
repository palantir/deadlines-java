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

import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.deadlines.Deadlines.ProvidedDeadline;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import javax.annotation.Nullable;

/**
 * Binds a {@link DeadlineContext} to the thread that opened the scope, until the scope is closed. Closing the scope
 * restores the context that was current on that thread when the scope was opened.
 * <p>
 * Obtain scopes from {@link Deadlines#withDeadline}, {@link Deadlines#withoutDeadline()} or
 * {@link DeadlineContext#attach()}, and close them with try-with-resources on the thread that opened them. Closing
 * a scope only changes the binding of that thread: work that captured the scope's context, for example with
 * {@link Deadlines#wrap(Runnable)}, keeps it.
 * <p>
 * Misuse never throws:
 * <ul>
 *   <li>Closing a scope on a thread other than the one that opened it does nothing and logs an error. The scope
 *       stays open on its thread.
 *   <li>Closing a scope while scopes opened after it on the same thread are still open closes those scopes too and
 *       logs an error.
 *   <li>Closing a scope that is already closed, including one closed by the previous rule, does nothing.
 * </ul>
 */
public final class DeadlineScope implements AutoCloseable {

    private static final SafeLogger log = SafeLoggerFactory.get(DeadlineScope.class);
    private static final RateLimiter misuseLogLimiter = RateLimiter.create(1.0);

    // The innermost binding of each thread. Each binding links to the binding it replaced, forming a stack.
    private static final ThreadLocal<Binding> currentBinding = new ThreadLocal<>();

    private final Thread thread;

    // Read and written only on `thread`.
    private boolean closed;

    private DeadlineScope(Thread thread) {
        this.thread = thread;
    }

    // Binds `state` on the current thread. A null state binds "no deadline".
    static DeadlineScope open(@Nullable ProvidedDeadline state) {
        DeadlineScope scope = new DeadlineScope(Thread.currentThread());
        currentBinding.set(new Binding(state, scope, currentBinding.get()));
        return scope;
    }

    // Null if no scope is open on the current thread.
    @Nullable
    static Binding currentBinding() {
        return currentBinding.get();
    }

    // Implements Deadlines.disableFurtherDeadlinePropagation for the current thread's binding. The replacement keeps
    // the same scope and previous binding, so it lasts until the innermost open scope closes.
    static void disablePropagationForCurrentBinding() {
        Binding binding = currentBinding.get();
        if (binding == null) {
            return;
        }
        ProvidedDeadline state = binding.state();
        if (state != null && !state.disablePropagation()) {
            currentBinding.set(new Binding(state.withPropagationDisabled(), binding.scope(), binding.previous()));
        }
    }

    /**
     * Restores the context that was current on this thread when this scope was opened. See the class documentation
     * for the behavior when scopes are closed out of order, on the wrong thread, or more than once. Never throws.
     */
    @Override
    public void close() {
        if (!thread.equals(Thread.currentThread())) {
            logMisuse("A DeadlineScope was closed on a thread other than the one that opened it. The scope remains"
                    + " open on the thread that opened it.");
            return;
        }
        if (closed) {
            return;
        }
        closed = true;
        // Pop this scope's binding and the bindings of any scopes opened after it that are still open.
        Binding binding = currentBinding.get();
        boolean closesInnerScopes = false;
        while (binding != null && binding.scope() != this) {
            closesInnerScopes = true;
            binding = binding.previous();
        }
        if (binding == null) {
            // Closing a scope opened before this one already popped this scope's binding.
            return;
        }
        Binding restored = binding.previous();
        if (restored == null) {
            currentBinding.remove();
        } else {
            currentBinding.set(restored);
        }
        if (closesInnerScopes) {
            logMisuse("A DeadlineScope was closed while scopes opened after it on the same thread were still open."
                    + " Those scopes were closed as well.");
        }
    }

    private static void logMisuse(@CompileTimeConstant String message) {
        if (misuseLogLimiter.tryAcquire()) {
            log.error(message, new SafeIllegalStateException(message));
        }
    }

    /** One entry of a thread's binding stack. A null state means "no deadline". */
    record Binding(
            @Nullable ProvidedDeadline state,
            DeadlineScope scope,
            @Nullable Binding previous) {}
}
