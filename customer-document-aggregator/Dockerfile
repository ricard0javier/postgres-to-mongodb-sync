FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build
COPY pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:go-offline
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /build/target/customer-document-aggregator.jar app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
