package com.pr.replication;

import org.testcontainers.containers.GenericContainer;

public class FollowerContainer extends GenericContainer<FollowerContainer> {

    public FollowerContainer() {
        super("replication-app:latest");
        withExposedPorts(8080);
        withEnv("SPRING_PROFILES_ACTIVE", "follower");
        withEnv("SERVER_PORT", "8080");
    }

    public String getBaseUrl() {
        return "http://" + getHost() + ":" + getMappedPort(8080);
    }
}

