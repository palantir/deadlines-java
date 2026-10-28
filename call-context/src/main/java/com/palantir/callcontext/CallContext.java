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

import com.google.errorprone.annotations.MustBeClosed;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.Safe;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;

/**
 * An immutable set of request-scoped values (each stored under a key) that are bound to threads. Values are read
 * and written only through the typed methods of this library (e.g. see {@code Deadlines}).
 * <ol>
 *   <li>Every thread has exactly one current context, returned by {@link #current()}. A thread with nothing bound has
 *       the {@link #empty()} context.
 *   <li>A thread's current context changes only when a {@link CallContextScope} is opened or closed on the thread.
 *   <li>Contexts are immutable: changing a value returns a new context. Nothing done on one thread changes the context
 *       another thread sees, except that {@link Deadlines#disarmRequestDeadline} disarms a request's deadline in
 *       every context derived from the request, on every thread.
 *   <li>Work handed to another thread carries a context only if it is wrapped with {@link #wrap(Runnable)},
 *       {@link #wrap(Callable)} or {@link #propagating(ExecutorService)}. Nothing else carries contexts (in particular,
 *       tracing does not carry contexts).
 * </ol>
 */
public final class CallContext {

    private static final CallContext EMPTY = new CallContext(new Entry<?>[0]);

    // Never modified after construction. Contexts hold few values, so lookups scan linearly.
    // Updates copy the array and reuse unchanged entries, avoiding the table and per-entry node allocations
    // of copying a HashMap.
    private final Entry<?>[] entries;

    private CallContext(Entry<?>[] entries) {
        this.entries = entries;
    }

    /** Returns the context with no values. */
    public static CallContext empty() {
        return EMPTY;
    }

    /**
     * Returns the context bound by the innermost {@link CallContextScope} open on this thread, or the
     * {@link #empty()} context if no scope is open.
     */
    public static CallContext current() {
        return CallContextScope.currentContext();
    }

    /** Returns the value stored under {@code key}, or null if this context has no value for it. */
    @Nullable
    <T> T get(Key<T> key) {
        int index = indexOf(key);
        if (index < 0) {
            return null;
        }
        @SuppressWarnings("unchecked") // with(Key<T>, T) is the only way to store a value under a Key<T>
        T value = (T) entries[index].value();
        return value;
    }

    /** Returns a context with the same values as this one, except that {@code key} maps to {@code value}. */
    <T> CallContext with(Key<T> key, T value) {
        Preconditions.checkNotNull(key, "key");
        Preconditions.checkNotNull(value, "value");
        int index = indexOf(key);
        Entry<?>[] newEntries;
        if (index >= 0) {
            newEntries = entries.clone();
            newEntries[index] = new Entry<>(key, value);
        } else {
            newEntries = Arrays.copyOf(entries, entries.length + 1);
            newEntries[entries.length] = new Entry<>(key, value);
        }
        return new CallContext(newEntries);
    }

    /** Returns a context with the same values as this one, except that it has no value for {@code key}. */
    CallContext without(Key<?> key) {
        int index = indexOf(key);
        if (index < 0) {
            return this;
        }
        if (entries.length == 1) {
            return EMPTY;
        }
        Entry<?>[] newEntries = new Entry<?>[entries.length - 1];
        System.arraycopy(entries, 0, newEntries, 0, index);
        System.arraycopy(entries, index + 1, newEntries, index, entries.length - index - 1);
        return new CallContext(newEntries);
    }

    /**
     * Opens a scope that binds this context on the current thread until the scope is closed. Close the scope on this
     * thread, which restores the context that was current before; see {@link CallContextScope}. Never throws.
     */
    @MustBeClosed
    public CallContextScope attach() {
        return CallContextScope.open(this);
    }

    /**
     * Wraps a task so that it runs with this context.
     * <p>
     * Each run binds this context on the running thread for the duration of the run, then restores the running
     * thread's previous context, even if the task throws. A scope the task leaves open is closed when the run ends.
     * To hand off the current context, use {@code CallContext.current().wrap(task)}.
     */
    public Runnable wrap(Runnable task) {
        Preconditions.checkNotNull(task, "task");
        return () -> {
            try (CallContextScope ignored = attach()) {
                task.run();
            }
        };
    }

    /** Wraps a task so that it runs with this context. Behaves exactly as {@link #wrap(Runnable)}. */
    public <V> Callable<V> wrap(Callable<V> task) {
        Preconditions.checkNotNull(task, "task");
        return () -> {
            try (CallContextScope ignored = attach()) {
                return task.call();
            }
        };
    }

    /**
     * Returns an executor service that runs each task with the context that was current on the submitting thread when
     * the task was passed to {@code execute}, {@code submit}, {@code invokeAll} or {@code invokeAny}.
     * <p>
     * Tasks are wrapped as described in {@link #wrap(Runnable)}, even if they never run, and
     * {@link ExecutorService#shutdownNow()} returns the wrapped tasks.
     */
    public static ExecutorService propagating(ExecutorService executor) {
        return new ContextPropagatingExecutorService(Preconditions.checkNotNull(executor, "executor"));
    }

    /** Lists the names of the keys this context has values for. Values are omitted because they may be unsafe. */
    @Override
    public String toString() {
        return "CallContext{keys=" + Arrays.toString(entries) + '}';
    }

    private int indexOf(Key<?> key) {
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].key() == key) {
                return i;
            }
        }
        return -1;
    }

    private record Entry<T>(Key<T> key, T value) {
        @Override
        @Safe
        public String toString() {
            // The default record toString would include the value, which may be unsafe.
            return key.toString();
        }
    }

    /**
     * Identifies a value in a {@link CallContext}. Keys have identity semantics: two keys created with the same name
     * are different keys. Keep a key private to the code that owns its values, and expose methods that read and write
     * those values instead.
     */
    @SuppressWarnings("UnusedTypeParameter") // T ties the key to the type of its values in get and with
    static final class Key<T> {

        private final String name;

        private Key(String name) {
            this.name = name;
        }

        /** Creates a key. The name is used only for debugging, for example in {@link CallContext#toString()}. */
        static <T> Key<T> create(@Safe String name) {
            return new Key<>(Preconditions.checkNotNull(name, "name"));
        }

        @Override
        @Safe
        public String toString() {
            return name;
        }
    }
}
