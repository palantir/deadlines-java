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

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

/**
 * Indicates that a deadline has expired.
 */
public abstract sealed class DeadlineExpiredException extends RuntimeException implements SafeLoggable {
    private static final List<Arg<?>> EMPTY_ARGS = List.of();

    private DeadlineExpiredException(String message) {
        super(message);
    }

    public abstract <T> T accept(Visitor<T> visitor);

    public static External external() {
        return new External();
    }

    public static Internal internal() {
        return new Internal();
    }

    /**
     * Indicates that a deadline has expired due to a server being unable to meet an externally provided deadline.
     */
    public static final class External extends DeadlineExpiredException implements SafeLoggable {
        private static final String MESSAGE = "An externally provided deadline for completing work has expired.";

        private External() {
            super(MESSAGE);
        }

        @Override
        public @Safe String getLogMessage() {
            return MESSAGE;
        }

        @Override
        public List<Arg<?>> getArgs() {
            return EMPTY_ARGS;
        }

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * Indicates that a deadline has expired due to a server being unable to meet an internally-imposed deadline.
     */
    public static final class Internal extends DeadlineExpiredException implements SafeLoggable {
        private static final String MESSAGE = "An internal deadline for completing work has expired.";

        private Internal() {
            super(MESSAGE);
        }

        @Override
        public @Safe String getLogMessage() {
            return MESSAGE;
        }

        @Override
        public List<Arg<?>> getArgs() {
            return EMPTY_ARGS;
        }

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    public interface Visitor<T> {
        T visit(External external);

        T visit(Internal internal);
    }

    public enum HttpResponseCodeVisitor implements Visitor<Integer> {
        INSTANCE;

        @Override
        public Integer visit(External _external) {
            return 400;
        }

        @Override
        public Integer visit(Internal _internal) {
            return 500;
        }
    }
}
