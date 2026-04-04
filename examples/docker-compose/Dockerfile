FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml ./fencepost/pom.xml
COPY src ./fencepost/src
RUN mvn -f fencepost/pom.xml install -q -DskipTests

COPY examples/docker-compose/pom.xml ./example/pom.xml
COPY examples/docker-compose/src ./example/src
RUN mvn -f example/pom.xml package -q -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/example/target/fencepost-example.jar app.jar
CMD ["java", "-jar", "app.jar"]
