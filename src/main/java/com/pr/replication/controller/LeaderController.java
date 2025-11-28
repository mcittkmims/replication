package com.pr.replication.controller;

import com.pr.replication.model.WriteRequest;
import com.pr.replication.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Profile("leader")
public class LeaderController {

    private final StorageService storageService;

    @PostMapping("/write")
    @ResponseStatus(HttpStatus.CREATED)
    public void replicate(@RequestBody WriteRequest writeRequest){
        storageService.write(writeRequest.key(), writeRequest.value());
    }

    @GetMapping("/{key}")
    public String getKey(@PathVariable String key){
        return storageService.get(key);
    }
}
