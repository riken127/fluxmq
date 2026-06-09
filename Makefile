.PHONY: format format-check lint test verify

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
