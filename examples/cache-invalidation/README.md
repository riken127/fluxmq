# FluxMQ Cache Invalidation Example

This example demonstrates a production-style FluxMQ pattern: publishing best-effort refresh hints
after durable state changes.

The important pieces are:

- `ProductRepository`: the source of truth;
- `ProductCache`: local derived state;
- `ProductService`: writes the repository, then publishes `ProductChanged`;
- `ProductCacheInvalidator`: listens for `catalog.product.changed` and evicts stale cache entries;
- `CacheInvalidationApplicationTest`: proves the end-to-end flow through FluxMQ.

If the FluxMQ message is missed, correctness still comes from the repository. The cache can recover
through TTLs, version checks, explicit reads, or reconciliation.

## Run

Build and install the root starter first:

```bash
../../mvnw install -DskipTests
```

Then run the example:

```bash
mvn spring-boot:run
```

## Test

```bash
mvn test
```
