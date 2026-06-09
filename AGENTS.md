@RTK.md

# FluxMQ Project Rules

Read this file before changing code in this repository.

FluxMQ is a lightweight Spring Boot compatible SDK for best-effort event-style messaging over
ZeroMQ/JeroMQ. Keep the library small, explicit, and easy to reason about.

## Engineering Rules

- Do not turn FluxMQ into a broker.
- Do not claim durable queues, message replay, consumer groups, exactly-once delivery, distributed
  acknowledgements, transactions, clustering, schema registry, or stream processing.
- Do not add dependencies casually. Document every non-JDK dependency in `docs/architecture.md`.
- Use Java 25 and Maven as the source of truth.
- Use Google Java Format through Spotless.
- Use Google Checkstyle.
- Run `make format` after code changes.
- Run `make verify` before handing off substantial changes.
- Public APIs need concise Javadocs.
- Prefer records for immutable data carriers.
- Prefer final classes with constructor dependencies.
- Prefer package-private implementation where Spring or package boundaries allow it.
- Avoid unnecessary reflection. The `@FluxMqListener` scanner is the intended narrow exception.
- Avoid Lombok, MapStruct, Guice, Dagger, CDI, large validation frameworks, and fake enterprise
  layering.
- Update `README.md` when commands, structure, dependencies, configuration, or messaging semantics
  change.
