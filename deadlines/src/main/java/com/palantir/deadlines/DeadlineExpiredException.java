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

import com.palantir.deadlines.Deadline.Origin;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Indicates that a deadline has expired.
 * <p>
 * When thrown by this library the exception carries the budget the request started with and how much time had
 * actually elapsed. Those are not the same number, because deadline expiration does not interrupt work in progress:
 * the elapsed time says how far past the budget the request had run by the time something noticed.
 */
public abstract sealed class DeadlineExpiredException extends RuntimeException implements SafeLoggable {
    private static final List<Arg<?>> EMPTY_ARGS = List.of();

    private final String logMessage;
    private final List<Arg<?>> args;

    private DeadlineExpiredException(String message, List<Arg<?>> args) {
        super(message);
        this.logMessage = message;
        this.args = args;
    }

    public static External external() {
        return new External(EMPTY_ARGS);
    }

    public static Internal internal() {
        return new Internal(EMPTY_ARGS);
    }

    static DeadlineExpiredException of(Origin origin, long budgetNanos, long elapsedNanos) {
        List<Arg<?>> args = List.of(
                SafeArg.of("deadlineMillis", TimeUnit.NANOSECONDS.toMillis(budgetNanos)),
                SafeArg.of("elapsedMillis", TimeUnit.NANOSECONDS.toMillis(elapsedNanos)),
                SafeArg.of("origin", origin));
        return switch (origin) {
            case INTERNAL -> new Internal(args);
            case EXTERNAL -> new External(args);
        };
    }

    @Override
    public final @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public final List<Arg<?>> getArgs() {
        return args;
    }

    /**
     * Indicates that a deadline has expired due to a server being unable to meet an externally provided deadline.
     */
    public static final class External extends DeadlineExpiredException {
        private static final String MESSAGE = "An externally provided deadline for completing work has expired.";

        private External(List<Arg<?>> args) {
            super(MESSAGE, args);
        }
    }

    /**
     * Indicates that a deadline has expired due to a server being unable to meet an internally-imposed deadline.
     */
    public static final class Internal extends DeadlineExpiredException {
        private static final String MESSAGE = "An internal deadline for completing work has expired.";

        private Internal(List<Arg<?>> args) {
            super(MESSAGE, args);
        }
    }
}
