package com.pr.replication.service;

import com.pr.replication.exception.WriteOperationFailedException;
import com.pr.replication.model.Pair;
import com.pr.replication.model.ReplicationRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class StorageService {
    private final Map<String, Pair<String, Instant>> concurrentMap= new ConcurrentHashMap<>();
    private final SenderService senderService;

    @Value("${replication.quorum:2}")
    private int quorum;

    public void write(String key, String value){
        Instant now = Instant.now();
        concurrentMap.put(key, new Pair<>(value, now));
        if (waitForQuorum(senderService.sendToAllFollowers(new ReplicationRequest(key, value, now)))){
            return;
        }
        throw new WriteOperationFailedException("Failure to reach specified quorum!");
    }

    public String get(String key){
        return concurrentMap.get(key).getFirst();
    }

    public void replicate(String key, String value, Instant time){
        concurrentMap.compute(key, (k, v) -> {
            if (v == null){
                return new Pair<>(value, time);
            }
            if (time.isAfter(v.getSecond())){
                return new Pair<>(value, time);
            }
            return v;
        });
    }

    private boolean waitForQuorum(List<CompletableFuture<Boolean>> futures) {

        AtomicInteger success = new AtomicInteger(0);
        CompletableFuture<Boolean> quorumFuture = new CompletableFuture<>();

        futures.forEach(f -> f.thenAccept(result -> {
            if (result && success.incrementAndGet() >= quorum) {
                quorumFuture.complete(true);
            }
        }));

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> quorumFuture.complete(false));

        return quorumFuture.join();
    }
}
