package com.pr.replication.controller;

import com.pr.replication.model.WriteRequest;
import com.pr.replication.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Profile("leader")
public class LeaderController {

    private final StorageService storageService;

    @PostMapping("/write")
    @ResponseStatus(HttpStatus.CREATED)
    public void replicate(@RequestBody WriteRequest writeRequest) {
        storageService.write(writeRequest.key(), writeRequest.value());
    }

    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> setConfig(@RequestBody Map<String, Object> config) {
        if (config.containsKey("writeQuorum")) {
            int newQuorum = ((Number) config.get("writeQuorum")).intValue();
            storageService.setQuorum(newQuorum);
            return ResponseEntity.ok(Map.of("success", true, "writeQuorum", newQuorum));
        }
        return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Unknown config key"));
    }

    @GetMapping("/config")
    public Map<String, Object> getConfig() {
        return Map.of("writeQuorum", storageService.getQuorum());
    }
}
