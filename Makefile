.PHONY: format format-check lint test verify verify-examples

format:
	./mvnw spotless:apply

format-check:
	./mvnw spotless:check

lint:
	./mvnw checkstyle:check

test:
	./mvnw test

verify:
	./mvnw verify spotless:check

verify-examples:
	./mvnw install -DskipTests
	mvn -f examples/basic-pubsub/pom.xml test
	mvn -f examples/cache-invalidation/pom.xml test
