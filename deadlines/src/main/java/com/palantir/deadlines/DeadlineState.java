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

import java.time.Duration;

/**
 * Immutable snapshot of a trace deadline. Values remain fixed after capture, including when read on another thread.
 */
public record DeadlineState(Duration remainingTime, Deadlines.Enforcement enforcement, Origin origin) {
    /** Origin of the deadline selected when parsing the request, independent of its enforcement strategy. */
    public enum Origin {
        /** The deadline was imposed internally by the server. */
        INTERNAL,
        /** The deadline was provided externally in the request. */
        EXTERNAL
    }
}
