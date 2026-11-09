# Multi-stage build for Java 21 Spring Boot Orchestrator
FROM maven:3.9-eclipse-temurin-21 as builder

WORKDIR /build
COPY orchestrator/pom.xml .
RUN mvn dependency:go-offline -B || true

COPY orchestrator/src ./src
RUN mvn clean package -DskipTests

# Minimal runtime image with Java 21
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/target/*.jar /app/orchestrator.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/orchestrator.jar"]

