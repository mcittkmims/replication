package com.pr.replication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
public class ReplicationApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReplicationApplication.class, args);
    }

}
