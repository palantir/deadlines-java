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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class CallContextTest {

    private static final CallContext.Key<String> NAME = CallContext.Key.create("name");
    private static final CallContext.Key<Integer> COUNT = CallContext.Key.create("count");

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ExecutorService otherExecutor = Executors.newSingleThreadExecutor();

    @AfterEach
    void afterEach() {
        executor.shutdownNow();
        otherExecutor.shutdownNow();
        assertThat(CallContextScope.currentScope())
                .as("the test left a scope open")
                .isNull();
    }

    @Nested
    class Values {

        @Test
        void the_empty_context_has_no_values() {
            assertThat(CallContext.empty().get(NAME)).isNull();
        }

        @Test
        void with_returns_a_new_context_and_leaves_the_original_unchanged() {
            CallContext original = CallContext.empty().with(NAME, "a");
            CallContext updated = original.with(NAME, "b").with(COUNT, 1);

            assertThat(original.get(NAME)).isEqualTo("a");
            assertThat(original.get(COUNT)).isNull();
            assertThat(updated.get(NAME)).isEqualTo("b");
            assertThat(updated.get(COUNT)).isEqualTo(1);
        }

        @Test
        void without_removes_only_that_key() {
            CallContext context = CallContext.empty().with(NAME, "a").with(COUNT, 1);

            CallContext withoutName = context.without(NAME);

            assertThat(withoutName.get(NAME)).isNull();
            assertThat(withoutName.get(COUNT)).isEqualTo(1);
            assertThat(context.get(NAME)).as("the original is unchanged").isEqualTo("a");
            assertThat(withoutName.without(NAME)).as("removing an absent key").isSameAs(withoutName);
            assertThat(withoutName.without(COUNT)).isSameAs(CallContext.empty());
        }

        @Test
        void keys_with_the_same_name_are_different_keys() {
            CallContext.Key<String> otherName = CallContext.Key.create("name");

            CallContext context = CallContext.empty().with(NAME, "a");

            assertThat(context.get(otherName)).isNull();
        }

        @Test
        void rejects_null_values() {
            assertThatThrownBy(() -> CallContext.empty().with(NAME, nullValue()))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void to_string_lists_key_names_but_not_values() {
            CallContext context = CallContext.empty().with(NAME, "secret").with(COUNT, 1);

            assertThat(context).hasToString("CallContext{keys=[name, count]}");
        }

        @SuppressWarnings("NullAway") // exercises the null check
        private @Nullable String nullValue() {
            return null;
        }
    }

    @Nested
    class Scopes {

        @Test
        void closing_restores_the_context_that_was_current_when_the_scope_opened() {
            CallContext outer = CallContext.empty().with(NAME, "outer");
            try (CallContextScope ignored = outer.attach()) {
                assertNestedScopesRestore(outer);
                assertThat(CallContext.current()).isSameAs(outer);
            }
            assertThat(CallContext.current()).isSameAs(CallContext.empty());
        }

        private void assertNestedScopesRestore(CallContext outer) {
            CallContext inner = outer.with(NAME, "inner");
            try (CallContextScope ignored = inner.attach()) {
                try (CallContextScope ignored1 = CallContext.empty().attach()) {
                    assertThat(CallContext.current()).isSameAs(CallContext.empty());
                }
                assertThat(CallContext.current()).isSameAs(inner);
            }
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // closes the scope on the wrong thread on purpose
        void closing_on_another_thread_does_nothing() throws Exception {
            CallContext context = CallContext.empty().with(NAME, "a");
            CallContextScope scope = context.attach();

            executor.submit(scope::close).get();

            assertThat(CallContext.current())
                    .as("the scope is still open on this thread")
                    .isSameAs(context);
            assertThat(executor.submit(CallContextScope::currentScope).get())
                    .as("nothing is bound on the other thread")
                    .isNull();
            scope.close();
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // closes scopes out of order on purpose
        void closing_a_scope_also_closes_scopes_opened_after_it() {
            CallContextScope outer = CallContext.empty().with(NAME, "outer").attach();
            CallContextScope inner = CallContext.empty().with(NAME, "inner").attach();

            outer.close();
            assertThat(CallContextScope.currentScope()).isNull();

            inner.close();
            assertThat(CallContextScope.currentScope())
                    .as("closing the inner scope later does not bind the outer scope's context again")
                    .isNull();
        }

        @Test
        void closing_twice_does_nothing() {
            CallContext outer = CallContext.empty().with(NAME, "outer");
            try (CallContextScope ignored = outer.attach()) {
                try (CallContextScope inner =
                        CallContext.empty().with(NAME, "inner").attach()) {
                    inner.close();
                }
                assertThat(CallContext.current()).isSameAs(outer);
            }
        }
    }

    @Nested
    class HandOffs {

        @Test
        void a_wrapped_task_runs_with_the_context_it_wraps() throws Exception {
            Callable<String> task;
            try (CallContextScope ignored = CallContext.empty().with(NAME, "a").attach()) {
                task = CallContext.current().wrap(() -> CallContext.current().get(NAME));
            }

            assertThat(executor.submit(task).get())
                    .as("runs after the scope closed, with the context captured when wrapped")
                    .isEqualTo("a");
        }

        @Test
        void a_wrapped_task_restores_the_running_threads_context_even_if_it_throws() {
            Runnable task = CallContext.empty().with(NAME, "task").wrap((Runnable) () -> {
                assertThat(CallContext.current().get(NAME)).isEqualTo("task");
                throw new SafeRuntimeException("task failed");
            });

            CallContext running = CallContext.empty().with(NAME, "running");
            try (CallContextScope ignored = running.attach()) {
                assertThatThrownBy(task::run).hasMessage("task failed");
                assertThat(CallContext.current()).isSameAs(running);
            }
        }

        @Test
        @SuppressWarnings("MustBeClosedChecker") // the task leaves its scope open on purpose
        void a_scope_left_open_by_a_wrapped_task_is_closed_when_the_task_ends() throws Exception {
            Runnable leaky = () -> CallContext.empty().with(NAME, "leaked").attach();

            executor.submit(CallContext.empty().wrap(leaky)).get();

            assertThat(executor.submit(CallContextScope::currentScope).get())
                    .as("nothing leaks into the next task on the same thread")
                    .isNull();
        }

        @Test
        void a_propagating_executor_captures_the_context_when_a_task_is_submitted() throws Exception {
            ExecutorService propagating = CallContext.propagating(executor);
            CountDownLatch release = new CountDownLatch(1);
            Future<?> blocker = executor.submit(() -> {
                release.await();
                return null;
            });

            Future<String> submittedInScope;
            try (CallContextScope ignored = CallContext.empty().with(NAME, "a").attach()) {
                submittedInScope =
                        propagating.submit(() -> CallContext.current().get(NAME));
            }
            Future<String> submittedOutsideScope =
                    propagating.submit(() -> CallContext.current().get(NAME));
            release.countDown();
            blocker.get();

            assertThat(submittedInScope.get())
                    .as("runs after the scope closed, with the context captured at submission")
                    .isEqualTo("a");
            assertThat(submittedOutsideScope.get()).isNull();
        }

        @Test
        void a_propagating_executor_wraps_every_task_passed_to_invoke_all() throws Exception {
            ExecutorService propagating = CallContext.propagating(executor);
            try (CallContextScope ignored = CallContext.empty().with(NAME, "a").attach()) {
                Callable<String> task = () -> CallContext.current().get(NAME);
                List<Future<String>> results = propagating.invokeAll(List.of(task, task));

                for (Future<String> result : results) {
                    assertThat(result.get()).isEqualTo("a");
                }
            }
        }

        @Test
        @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
        void closing_a_propagating_executor_uses_the_delegates_close() throws Exception {
            // The interface's default close() never returns for the common pool, whose own close() does nothing.
            ((AutoCloseable) CallContext.propagating(ForkJoinPool.commonPool())).close();
        }

        @Test
        void unwrapped_hand_offs_do_not_carry_contexts() throws Exception {
            try (CallContextScope ignored = CallContext.empty().with(NAME, "a").attach()) {
                assertThat(executor.submit(() -> CallContext.current().get(NAME))
                                .get())
                        .as("a plain executor carries nothing")
                        .isNull();
                // The default executor of CompletableFuture's async methods.
                assertThat(CompletableFuture.supplyAsync(
                                        () -> CallContext.current().get(NAME), ForkJoinPool.commonPool())
                                .get())
                        .as("the common pool carries nothing")
                        .isNull();
            }
        }

        @Test
        void a_context_bound_on_one_thread_never_affects_another_thread() throws Exception {
            ExecutorService propagating = CallContext.propagating(executor);
            ExecutorService otherPropagating = CallContext.propagating(otherExecutor);
            CountDownLatch loadDetached = new CountDownLatch(1);
            CountDownLatch backgroundChecked = new CountDownLatch(1);
            CallContext request = CallContext.empty().with(NAME, "a").with(COUNT, 1);

            try (CallContextScope ignored = request.attach()) {
                Future<CallContext> backgroundSees = propagating.submit(() -> {
                    loadDetached.await();
                    return CallContext.current();
                });
                Future<CallContext> loadSees = otherPropagating.submit(() -> {
                    try (CallContextScope ignored1 =
                            CallContext.current().without(NAME).attach()) {
                        loadDetached.countDown();
                        backgroundChecked.await();
                        return CallContext.current();
                    }
                });

                assertThat(backgroundSees.get())
                        .as("background work keeps the request's context while another task drops a value")
                        .isSameAs(request);
                backgroundChecked.countDown();
                CallContext load = loadSees.get();
                assertThat(load.get(NAME)).isNull();
                assertThat(load.get(COUNT)).as("other values are kept").isEqualTo(1);
                assertThat(CallContext.current())
                        .as("the request thread keeps its context")
                        .isSameAs(request);
            }
        }
    }
}
