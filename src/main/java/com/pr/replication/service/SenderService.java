package com.pr.replication.service;

import com.pr.replication.model.ReplicationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class SenderService {

    private final RestTemplate restTemplate;
    private SenderService self;

    @Value("${replication.followers:}")
    private List<String> followers;

    @Value("${follower.endpoint:/replicate}")
    private String endpoint;

    public SenderService(@Lazy SenderService self, RestTemplate restTemplate) {
        this.self = self;
        this.restTemplate = restTemplate;
    }

    @Async
    public CompletableFuture<Boolean> sendReplicationAsync(String url, ReplicationRequest body) {
        try {
            //Thread.sleep(ThreadLocalRandom.current().nextInt(minDelay, maxDelay + 1));
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
