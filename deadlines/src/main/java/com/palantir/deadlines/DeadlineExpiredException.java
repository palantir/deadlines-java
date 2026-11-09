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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.tracing.TraceMetadata;
import com.palantir.tracing.Tracer;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Indicates that a deadline has expired.
 */
public abstract sealed class DeadlineExpiredException extends RuntimeException implements SafeLoggable {
    @Nullable
    private final String requestId;

    private DeadlineExpiredException(String message) {
        super(message);
        this.requestId = Tracer.maybeGetTraceMetadata()
                .flatMap(TraceMetadata::getRequestId)
                .orElse(null);
    }

    /**
     * Returns the request ID of the trace that was current when this exception was created, if any.
     * <p>
     * An exception shared between requests, for example through a cache or future, retains the ID of the request
     * that created it. A request ID that differs from the current request's indicates that the expired deadline
     * belonged to another request.
     */
    public Optional<String> getRequestId() {
        return Optional.ofNullable(requestId);
    }

    @Override
    public List<Arg<?>> getArgs() {
        return requestId == null ? List.of() : List.of(SafeArg.of("requestId", requestId));
    }

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
    }
}
