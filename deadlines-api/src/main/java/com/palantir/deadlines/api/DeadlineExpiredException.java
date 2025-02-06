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
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

/**
 * Indicates that a deadline has expired.
 */
public abstract class DeadlineExpiredException extends RuntimeException {
    private DeadlineExpiredException(String message) {
        super(message);
    }

    public static External external() {
        return new External();
    }

    public static Internal internal() {
        return new Internal();
    }

    public static final class External extends DeadlineExpiredException implements SafeLoggable {
        private static final String MESSAGE = "An externally provided deadline for completing work has expired.";
        private static final List<Arg<?>> ARGS = List.of();

        private External() {
            super(MESSAGE);
        }

        @Override
        public @Safe String getLogMessage() {
            return MESSAGE;
        }

        @Override
        public List<Arg<?>> getArgs() {
            return ARGS;
        }
    }

    public static final class Internal extends DeadlineExpiredException implements SafeLoggable {
        private static final String MESSAGE = "An internal deadline for completing work has expired.";
        private static final List<Arg<?>> ARGS = List.of();

        private Internal() {
            super(MESSAGE);
        }

        @Override
        public @Safe String getLogMessage() {
            return MESSAGE;
        }

        @Override
        public List<Arg<?>> getArgs() {
            return ARGS;
        }
    }
}
