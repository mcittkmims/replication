package com.pr.replication.service;

import com.pr.replication.exception.WriteOperationFailedException;
import com.pr.replication.model.Pair;
import com.pr.replication.model.ReplicationRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class StorageService {
    private final Map<String, Pair<String, Instant>> concurrentMap = new ConcurrentHashMap<>();
    private final SenderService senderService;

    @Getter
    @Setter
    @Value("${replication.quorum:0}")
    private int quorum;

    @Value("${replication.version:false}")
    private boolean withVersion;

    public void write(String key, String value) {
        Instant now = Instant.now();
        concurrentMap.put(key, new Pair<>(value, now));
        if (waitForQuorum(senderService.sendToAllFollowers(new ReplicationRequest(key, value, now)))) {
            return;
        }
        throw new WriteOperationFailedException("Failure to reach specified quorum!");
    }

    public String get(String key) {
        return concurrentMap.getOrDefault(key, new Pair<>("", Instant.now())).getFirst();
    }

    public void replicate(String key, String value, Instant time) {
        concurrentMap.compute(key, (k, v) -> {
            if (!withVersion)
                return new Pair<>(value, time);

            if (v == null)
                return new Pair<>(value, time);

            if (time.isAfter(v.getSecond()))
                return new Pair<>(value, time);

            return v;
        });
    }

    private boolean waitForQuorum(List<CompletableFuture<Boolean>> futures) {

        AtomicInteger ok = new AtomicInteger(0);
        AtomicBoolean failed = new AtomicBoolean(false);

        CompletableFuture<Void> done = new CompletableFuture<>();

        for (CompletableFuture<Boolean> f : futures) {
            f.thenAccept(result -> {
                if (done.isDone()) return;

                if (!result) {
                    failed.set(true);
                    done.complete(null);
                    return;
                }

                if (ok.incrementAndGet() >= quorum) {
                    done.complete(null);
                }
            });
        }

        done.join();
        return !failed.get() && ok.get() >= quorum;
    }

    public Map<String, String> getAllData() {
        Map<String, String> result = new HashMap<>();
        concurrentMap.forEach((key, pair) -> result.put(key, pair.getFirst()));
        return result;
    }

    public Map<String, Long> getAllVersions() {
        Map<String, Long> result = new HashMap<>();
        concurrentMap.forEach((key, pair) -> result.put(key, pair.getSecond().toEpochMilli()));
        return result;
    }
}
