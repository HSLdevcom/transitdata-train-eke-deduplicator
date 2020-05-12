FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-train-eke-deduplicator.jar /usr/app/transitdata-train-eke-deduplicator.jar
ENTRYPOINT ["java", "-Xms256m", "-Xmx4096m", "-jar", "/usr/app/transitdata-train-eke-deduplicator.jar"]
