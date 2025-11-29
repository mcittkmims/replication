package com.pr.replication.integration;

import com.pr.replication.FollowerContainer;
import com.pr.replication.model.WriteRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("leader")
@Testcontainers
class ReplicationIntegrationTest {

    private static final int FOLLOWER_COUNT = 5;

    static List<FollowerContainer> followers = new ArrayList<>();

    @LocalServerPort
    int leaderPort;

    @Autowired
    TestRestTemplate leaderClient;

    @DynamicPropertySource
    static void followerProps(DynamicPropertyRegistry registry) {

        for (int i = 0; i < FOLLOWER_COUNT; i++) {
            FollowerContainer c = new FollowerContainer();
            c.start();
            followers.add(c);
        }

        String followerUrls = followers.stream()
                .map(FollowerContainer::getBaseUrl)
                .collect(Collectors.joining(","));

        registry.add("replication.followers", () -> followerUrls);
        registry.add("replication.quorum", () -> "3");
        registry.add("replication.version", () -> "false");
        registry.add("delay.simulation", () -> "true");


    }

    @AfterAll
    static void shutdownFollowers() {
        followers.forEach(GenericContainer::stop);
    }

    @Test
    void givenLeader_whenWritingAndReading_thenReturnsCorrectValue() {
        WriteRequest req = new WriteRequest("x", "100");

        ResponseEntity<Void> resp =
                leaderClient.postForEntity("/write", req, Void.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> read =
                leaderClient.getForEntity("/x", String.class);

        assertThat(read.getBody()).isEqualTo("100");
    }

    @Test
    void givenLeader_whenWritingData_thenFollowersReceiveReplicatedData() throws Exception {
        WriteRequest req = new WriteRequest("hello", "world");

        ResponseEntity<Void> resp =
                leaderClient.postForEntity("/write", req, Void.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        Thread.sleep(1000);

        RestTemplate client = new RestTemplate();

        for (FollowerContainer c : followers) {
            ResponseEntity<String> result =
                    client.getForEntity(c.getBaseUrl() + "/hello", String.class);
            assertThat(result.getBody()).isEqualTo("world");
        }
    }

    @Test
    void givenFollower_whenAttemptingWrite_thenRequestIsRejected() {
        RestTemplate client = new RestTemplate();
        FollowerContainer follower = followers.get(0);

        WriteRequest wr = new WriteRequest("k", "v");

        assertThatThrownBy(() ->
                client.postForEntity(follower.getBaseUrl() + "/write", wr, Void.class)
        ).isInstanceOf(HttpClientErrorException.MethodNotAllowed.class)
                .extracting("statusCode")
                .isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void givenLeader_whenReceivingConcurrentWrites_thenAllWritesSucceed() throws Exception {

        int threadCount = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        List<Callable<ResponseEntity<Void>>> tasks = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            int id = i;
            tasks.add(() ->
                    leaderClient.postForEntity(
                            "/write",
                            new WriteRequest("k" + id, "v" + id),
                            Void.class
                    )
            );
        }

        List<Future<ResponseEntity<Void>>> results = pool.invokeAll(tasks);

        for (Future<ResponseEntity<Void>> f : results) {
            assertThat(f.get().getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
    }

    @Test
    void givenCluster_whenCheckingHealth_thenAllNodesAreHealthy() {
        RestTemplate client = new RestTemplate();

        // Leader health
        ResponseEntity<String> leaderHealth =
                client.getForEntity("http://localhost:" + leaderPort + "/actuator/health", String.class);
        assertThat(leaderHealth.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Followers health
        for (FollowerContainer f : followers) {
            ResponseEntity<String> h =
                    client.getForEntity(f.getBaseUrl() + "/actuator/health", String.class);
            assertThat(h.getStatusCode()).isEqualTo(HttpStatus.OK);
        }
    }

    @Test
    void givenSameKey_whenWrittenMultipleTimes_thenFinalValueIsConsistentAcrossCluster() throws Exception {

        leaderClient.postForEntity("/write", new WriteRequest("shared", "v1"), Void.class);
        leaderClient.postForEntity("/write", new WriteRequest("shared", "v2"), Void.class);
        leaderClient.postForEntity("/write", new WriteRequest("shared", "v3"), Void.class);
        leaderClient.postForEntity("/write", new WriteRequest("shared", "v4"), Void.class);
        leaderClient.postForEntity("/write", new WriteRequest("shared", "v5"), Void.class);

        Thread.sleep(1000);

        RestTemplate client = new RestTemplate();

        // leader should have v3
        ResponseEntity<String> leaderVal =
                leaderClient.getForEntity("/shared", String.class);
        assertThat(leaderVal.getBody()).isEqualTo("v5");

        // all followers should also have v3
        for (FollowerContainer f : followers) {
            ResponseEntity<String> r =
                    client.getForEntity(f.getBaseUrl() + "/shared", String.class);
            assertThat(r.getBody()).isEqualTo("v5");
        }
    }
}
