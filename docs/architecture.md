# FluxMQ Architecture

## Design Philosophy

FluxMQ is intentionally small. It exposes a publishing interface, a listener annotation, immutable
headers/envelopes, Jackson serialization, and a narrow Spring Boot auto-configuration layer.

The implementation favors explicit constructor wiring, final classes, realistic tests, and clear
package boundaries over framework-like extension machinery.

## Package Boundaries

- `io.fluxmq`: public API types.
- `io.fluxmq.autoconfigure`: Spring Boot properties and auto-configuration.
- `io.fluxmq.listener`: listener discovery, validation, registry, and invocation metadata.
- `io.fluxmq.serialization`: named serializers, Jackson payload serialization, and envelope metadata
  encoding.
- `io.fluxmq.transport`: JeroMQ PUB/SUB socket lifecycle and receive loop.
- `io.fluxmq.error`: default error handling.

## Dependency Decisions

Dependency: Spring Boot auto-configuration  
Scope: compile  
Problem solved: integrates FluxMQ with Spring Boot property binding and conditional beans.  
Why existing stack was insufficient: the library is meant to feel native in Spring Boot apps.  
Why this dependency is lightweight enough: only Boot core/autoconfigure APIs are used.  
Alternatives rejected: manual application wiring only, broad annotation framework.

Dependency: JeroMQ  
Scope: compile  
Problem solved: pure Java ZeroMQ-compatible PUB/SUB transport.  
Why existing stack was insufficient: the JDK does not provide ZeroMQ sockets.  
Why this dependency is lightweight enough: it avoids native libzmq installation for tests and apps.  
Alternatives rejected: native JZMQ bindings, broker clients such as Kafka or RabbitMQ.

Dependency: Jackson databind and JSR-310 module  
Scope: compile  
Problem solved: JSON payload and envelope metadata serialization, including `Instant`.  
Why existing stack was insufficient: the JDK does not provide JSON object binding.  
Why this dependency is lightweight enough: Jackson is the default JSON stack in Spring Boot.  
Alternatives rejected: Gson, JSON-B, custom JSON handling.

Dependency: SLF4J API  
Scope: compile  
Problem solved: default error handler logging without choosing a logging backend.  
Why existing stack was insufficient: the JDK logger is not the common Spring Boot facade.  
Why this dependency is lightweight enough: it is only an API and Boot supplies a provider.  
Alternatives rejected: direct Logback dependency, `java.util.logging`.

Dependency: JUnit 5  
Scope: test  
Problem solved: unit and integration testing.  
Why existing stack was insufficient: the JDK has no test runner.  
Why this dependency is lightweight enough: it is the standard modern Java test framework.  
Alternatives rejected: JUnit 4, TestNG.

Dependency: AssertJ  
Scope: test  
Problem solved: readable assertions for configuration and messaging tests.  
Why existing stack was insufficient: JUnit assertions are less expressive for object graphs.  
Why this dependency is lightweight enough: test-only dependency.  
Alternatives rejected: Hamcrest-only assertions.

Dependency: Spring Boot test and test auto-configuration  
Scope: test  
Problem solved: validates auto-configuration with `ApplicationContextRunner`.  
Why existing stack was insufficient: manually bootstrapping Boot conditions is brittle.  
Why this dependency is lightweight enough: test-only and avoids full application startup.  
Alternatives rejected: `spring-boot-starter-test` because it pulls in Mockito and broader extras.

Dependency: Spring Boot test in examples  
Scope: example test  
Problem solved: validates example applications with real Spring Boot contexts.  
Why existing stack was insufficient: example tests need to prove the documented auto-configuration
and listener flow work end to end.  
Why this dependency is lightweight enough: example-only test dependency; examples avoid
`spring-boot-starter-test` and use explicit JUnit and AssertJ dependencies.  
Alternatives rejected: untested examples, broad `spring-boot-starter-test`.

Dependency: Spotless and Google Java Format  
Scope: build  
Problem solved: deterministic Java formatting.  
Why existing stack was insufficient: Maven has no built-in formatter.  
Why this dependency is lightweight enough: build-time only.  
Alternatives rejected: IDE-specific formatting.

Dependency: Google Checkstyle  
Scope: build  
Problem solved: linting against a known Java style baseline.  
Why existing stack was insufficient: javac warnings do not cover style rules.  
Why this dependency is lightweight enough: build-time only.  
Alternatives rejected: custom style checks.

Dependency: Maven Source Plugin  
Scope: release build  
Problem solved: attaches source jars required by Maven Central.  
Why existing stack was insufficient: Maven does not attach source jars by default.  
Why this dependency is lightweight enough: release-profile only.  
Alternatives rejected: hand-built source archives.

Dependency: Maven Javadoc Plugin  
Scope: release build  
Problem solved: attaches Javadoc jars required by Maven Central.  
Why existing stack was insufficient: Maven does not attach Javadoc jars by default.  
Why this dependency is lightweight enough: release-profile only.  
Alternatives rejected: hand-built documentation jars.

Dependency: Maven GPG Plugin  
Scope: release build  
Problem solved: signs published artifacts for Maven Central validation.  
Why existing stack was insufficient: Maven does not sign artifacts by default.  
Why this dependency is lightweight enough: release-profile only.  
Alternatives rejected: manual signing outside the build.

Dependency: Central Publishing Maven Plugin  
Scope: release build  
Problem solved: uploads and publishes release bundles through the Maven Central Publisher Portal.  
Why existing stack was insufficient: legacy OSSRH deployment is no longer the preferred Central flow.  
Why this dependency is lightweight enough: release-profile only.  
Alternatives rejected: manual Portal uploads, legacy OSSRH staging plugin.

## Why JeroMQ

JeroMQ keeps the MVP self-contained and testable. It gives FluxMQ ZeroMQ PUB/SUB behavior without
native library installation, Docker, or Testcontainers.

## Why Spring Boot Auto-Configuration Is Allowed

The purpose of FluxMQ is Spring Boot compatibility. Spring usage is limited to configuration
properties, conditional bean wiring, and lifecycle integration. FluxMQ does not use Spring to invent
a larger runtime framework.

## Broker-Like Semantics Are Omitted

Durability, replay, consumer groups, acknowledgements, transactions, and clustering require storage,
coordination, and protocol semantics outside ZeroMQ PUB/SUB. FluxMQ omits them so the library remains
honest about what it can guarantee.

## Threading Model

FluxMQ uses one publisher socket and one subscriber receive loop. The publisher socket is owned by a
dedicated daemon worker thread and accepts send commands through a bounded queue. `publish` remains a
synchronous API: it waits for the worker to send or fail the command.

Listener invocation is synchronous on the subscriber thread. There are no async handler pools and no
unbounded executors. Applications should still avoid treating ZeroMQ sockets as general-purpose
cross-thread objects.

## Serialization Extension Point

The public serialization contract remains `FluxMqSerializer`. Auto-configuration selects the default
serializer from `FluxMqSerializerRegistry`, which includes `json`, `string`, and `bytes`. Applications
can replace the active serializer bean or supply their own registry.

## Observability Extension Point

`FluxMqObservationHandler` receives publish success/failure and listener success/failure callbacks.
The default implementation is no-op. This gives applications a place to bridge into Micrometer,
OpenTelemetry, logs, or custom counters without FluxMQ depending on those backends.

## Retry Boundary

`fluxmq.retry.*` is a local listener retry policy. It repeats synchronous handler invocation in the
subscriber process before calling `FluxMqErrorHandler`. It does not change ZeroMQ delivery semantics,
does not persist data, and does not ask the publisher to resend.

## Known Limitations

- Best-effort delivery only.
- No transport redelivery or dead-letter flow.
- No wildcard routing.
- No schema registry.
- No metrics or tracing backend.
- Listener scanning only covers ordinary Spring bean methods.

## Future Extension Points

- Optional metrics/tracing adapters built on `FluxMqObservationHandler`.
- Optional retry policies richer than fixed local attempts.
