# First stage: JDK with GraalVM
FROM ghcr.io/graalvm/native-image-community:21 AS build

WORKDIR /usr/src/app

# Copy pom.xml and download dependencies
#COPY pom.xml .
#RUN mvn dependency:go-offline

RUN microdnf install findutils

COPY . .

RUN ./gradlew build -x test

# Second stage: Lightweight debian-slim image
FROM ghcr.io/graalvm/jdk-community:21

WORKDIR /app

# Copy the native binary from the build stage
COPY --from=build /usr/src/app/build/libs/cdc-0.0.1-SNAPSHOT.jar /app/dedupe.jar

COPY config.yaml ./config.yaml

# Run the application
CMD ["java", "-jar", "/app/dedupe.jar","watch","--config", "/app/config.yaml"]