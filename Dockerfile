FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

COPY mvnw .
COPY .mvn .mvn

COPY pom.xml .
COPY src src

RUN ./mvnw -q -DskipTests package


FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY --from=builder /build/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
