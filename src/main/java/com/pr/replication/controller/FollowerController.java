package com.pr.replication.controller;

import com.pr.replication.model.ReplicationRequest;
import com.pr.replication.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Profile("follower")
public class FollowerController {

    private final StorageService storageService;

    @PostMapping("/replicate")
    @ResponseStatus(HttpStatus.CREATED)
    public void replicate(@RequestBody ReplicationRequest replication) {
        storageService.replicate(replication.key(), replication.value(), replication.time());
    }
}
