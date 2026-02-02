<p align="right">
<a href="https://autorelease.general.dmz.palantir.tech/palantir/deadlines-java"><img src="https://img.shields.io/badge/Perform%20an-Autorelease-success.svg" alt="Autorelease"></a>
</p>

# Deadlines [![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)

_Deadlines is a library for implementing soft deadlines for [Dialogue](https://github.com/palantir/dialogue)-based RPC._

## Overview

Deadlines allows clients to communicate time constraints to servers through HTTP headers, enabling servers to fail fast and avoid wasting resources on downstream RPC calls when results will arrive too late to be useful. Deadline values are automatically propagated through multi-hop request chains, and enforcement strategies can be configured at both the service and client layers.

**Important**: Deadline expiration does not preempt or terminate work that is already in-flight. It only prevents new outbound RPC calls from being initiated, providing backpressure to prevent cascading downstream calls.

## Usage

### Automatic Integration with Dialogue

**Most users don't need to use this library directly.** When using [Dialogue](https://github.com/palantir/dialogue) clients, deadline propagation happens automatically:

- **Dialogue clients** automatically encode deadlines into the `Expect-Within` header based on socket read timeout or current trace context.
- **Servers** can parse `Expect-Within` headers from incoming requests and propagate deadline state to outbound Dialogue requests. If using Witchcraft, this parsing and propagation is handled automatically.

### Configuring Enforcement

By default, deadlines are propagated but not enforced (DEFER mode). To enable enforcement, configure your Dialogue client:

```java
// Using Dialogue's ReloadingFactory
DialogueClients.ReloadingFactory factory = DialogueClients.create(servicesConfig)
    .withUserAgent(agent)
    .withDeadlineEnforcement(true);  // Enable enforcement

FooServiceBlocking client = factory.get(FooServiceBlocking.class, "foo-service");
```

## Enforcement Modes

Deadlines supports three enforcement strategies:

### ENFORCE
When a deadline expires, subsequent outbound requests will throw a `DeadlineExpiredException`. This mode also propagates the enforcement request to downstream services via the `Expect-Within-Enforced: true` header.

### DEFER
Deadline expiration will only throw an exception if an upstream service explicitly requested enforcement via the `Expect-Within-Enforced: true` header. Otherwise, the deadline is tracked but not enforced. This is the default and safest mode for initial adoption.

### DISABLE
Deadline enforcement is disabled for this node and all downstream services. The `Expect-Within-Enforced: false` header is sent to downstream services to indicate they should not enforce deadlines either.

This mode **overrides all other enforcement strategies**, disabling enforcement even if an upstream service requested it. This can be useful for protecting critical workflows that should always complete.

## HTTP Headers

Deadlines uses the following HTTP headers:

- **`Expect-Within`**: Client-provided deadline value in decimal seconds (e.g., "5.0" for 5 seconds)
- **`Expect-Within-Enforced`**: Boolean flag ("true" or "false") indicating whether enforcement is requested
- **`Deadline-Expired-Reason`**: Included in error responses to indicate whether an "external" or "internal" deadline was exceeded

## Exceptions

### DeadlineExpiredException

Thrown when a deadline expires and enforcement is enabled. This exception has two variants:

- **`DeadlineExpiredException.External`**: Thrown when an externally-provided (client-side) deadline expires. Results in a 400 status code.
- **`DeadlineExpiredException.Internal`**: Thrown when an internally-imposed (server-side) deadline expires. Results in a 500 status code.

## Adoption Considerations

Deadlines are most beneficial for request chains with multiple downstream services where tail latency matters and preventing cascading failures is critical.

**Important caveats:**
- **Fire & forget workflows**: If your service initiates asynchronous work or makes end-of-request calls which are expected to succeed even after the client has stopped waiting, these operations may fail if a deadline expires and enforcement prevents outbound RPC calls. To allow these operations to complete, either call `Deadlines.disableFurtherDeadlinePropagation()` before initiating (which stops deadline headers from being added to subsequent requests), or configure specific clients with `Enforcement.DISABLE`.
- **Non-preemptive**: Deadline expiration does not terminate in-flight work. It only prevents new outbound requests, providing backpressure while allowing ongoing work to finish gracefully.

## License

This project is made available under the [Apache 2.0 License](LICENSE).
