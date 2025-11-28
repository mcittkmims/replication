package com.pr.replication.model;

import java.time.Instant;

public record ReplicationRequest(String key, String value, Instant time) {
}
