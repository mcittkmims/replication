package com.pr.replication.controller;

import com.pr.replication.service.SenderService;
import com.pr.replication.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class GeneralController {
    private final StorageService storageService;

    @GetMapping("/{key}")
    public String getKey(@PathVariable String key) {
        return storageService.get(key);
    }

    @GetMapping("/dump")
    public Map<String, String> dump() {
        return storageService.getAllData();
    }

    @GetMapping("/dump-versions")
    public Map<String, Long> dumpVersions() {
        return storageService.getAllVersions();
    }
}
