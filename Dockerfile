# --- Build stage ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B clean package -DskipTests

# --- Runtime stage ---
# JRE only (not full JDK) + Alpine to keep the image, and therefore the process's
# baseline memory/footprint, small.
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/chat-message-persistence.jar app.jar

# Conservative default heap sizing - this workload is a single-threaded, streaming
# poll/persist loop, it does not need a large heap. Override via JAVA_OPTS if needed.
ENV JAVA_OPTS="-Xms64m -Xmx256m"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
