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

package com.palantir.deadlines.api;

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

/**
 * Indicates that a deadline has expired.
 */
public final class DeadlineExpiredException extends RuntimeException implements SafeLoggable {
    private static final String MESSAGE = "A deadline for completing work has expired.";
    private final List<Arg<?>> args;
    private final boolean internal;

    /**
     * Create a new DeadlineExpiredException.
     *
     * @param internal true if this is expiration was for an internally-sourced deadline (such as one assigned
     * internally by a server), or false if it was for a externally-sourced deadline (such as one provided by a
     * client in a request header)
     */
    public DeadlineExpiredException(boolean internal) {
        this.args = List.of(SafeArg.of("internal", internal));
        this.internal = internal;
    }

    @Override
    public @Safe String getLogMessage() {
        return MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }

    public boolean isInternal() {
        return internal;
    }
}
