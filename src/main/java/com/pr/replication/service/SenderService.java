package com.pr.replication.service;

import com.pr.replication.model.ReplicationRequest;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Log4j2
public class SenderService {

    private static final int MIN_DELAY = 0;
    private static final int MAX_DELAY = 1000;

    private final RestTemplate restTemplate;
    private SenderService self;

    @Value("${replication.followers:}")
    private List<String> followers;

    @Value("${follower.endpoint:/replicate}")
    private String endpoint;

    @Value("${delay.simulation:false}")
    private boolean delay;


    public SenderService(@Lazy SenderService self, RestTemplate restTemplate) {
        this.self = self;
        this.restTemplate = restTemplate;
    }

    @Async
    public CompletableFuture<Boolean> sendReplicationAsync(String url, ReplicationRequest body) {
        try {
            if (delay) Thread.sleep(ThreadLocalRandom.current().nextInt(MIN_DELAY, MAX_DELAY + 1));
            restTemplate.postForEntity(url + endpoint, body, Void.class);
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    public List<CompletableFuture<Boolean>> sendToAllFollowers(ReplicationRequest body) {
        return followers.stream()
                .map(url -> self.sendReplicationAsync(url, body))
                .toList();
    }
}
