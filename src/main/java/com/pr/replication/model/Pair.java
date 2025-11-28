package com.pr.replication.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Pair<K,V> {
    private K first;
    private V second;
}
