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

package com.palantir.callcontext;

import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import javax.annotation.Nullable;

/**
 * Binds a {@link CallContext} to the thread that opened the scope, until the scope is closed. Closing the scope
 * restores the context that was current on that thread when the scope was opened.
 * <p>
 * Obtain scopes from {@link CallContext#attach()}, and close them with try-with-resources on the thread that opened
 * them. Closing a scope only changes the binding of that thread: work that captured the scope's context, for example
 * with {@link CallContext#wrap(Runnable)}, keeps it.
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
public final class CallContextScope implements AutoCloseable {

    private static final SafeLogger log = SafeLoggerFactory.get(CallContextScope.class);
    private static final RateLimiter misuseLogLimiter = RateLimiter.create(1.0);

    // The innermost open scope of each thread. Each scope links to the scope that was innermost when it opened,
    // forming a stack.
    private static final ThreadLocal<CallContextScope> currentScope = new ThreadLocal<>();

    private final CallContext context;
    private final Thread thread;

    @Nullable
    private final CallContextScope previous;

    private CallContextScope(CallContext context, Thread thread, @Nullable CallContextScope previous) {
        this.context = context;
        this.thread = thread;
        this.previous = previous;
    }

    static CallContextScope open(CallContext context) {
        CallContextScope scope = new CallContextScope(context, Thread.currentThread(), currentScope.get());
        currentScope.set(scope);
        return scope;
    }

    static CallContext currentContext() {
        CallContextScope scope = currentScope.get();
        return scope == null ? CallContext.empty() : scope.context;
    }

    // Null if no scope is open on the current thread.
    @Nullable
    static CallContextScope currentScope() {
        return currentScope.get();
    }

    /**
     * Restores the context that was current on this thread when this scope was opened. See the class documentation
     * for the behavior when scopes are closed out of order, on the wrong thread, or more than once. Never throws.
     */
    @Override
    public void close() {
        if (!thread.equals(Thread.currentThread())) {
            logMisuse("A CallContextScope was closed on a thread other than the one that opened it. The scope"
                    + " remains open on the thread that opened it.");
            return;
        }
        // Pop this scope and any scopes opened after it that are still open.
        CallContextScope innermost = currentScope.get();
        CallContextScope scope = innermost;
        while (scope != this) {
            if (scope == null) {
                // This scope was already closed, directly or by closing a scope opened before it.
                return;
            }
            scope = scope.previous;
        }
        currentScope.set(previous);
        if (innermost != this) {
            logMisuse("A CallContextScope was closed while scopes opened after it on the same thread were still"
                    + " open. Those scopes were closed as well.");
        }
    }

    private static void logMisuse(@CompileTimeConstant String message) {
        if (misuseLogLimiter.tryAcquire()) {
            log.error(message, new SafeIllegalStateException(message));
        }
    }
}
