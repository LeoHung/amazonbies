package com.mastertheboss.undertow.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import java.util.concurrent.ConcurrentMap;

public class LRUConcurrentCache<K, V> implements MyCache<K, V>{
    private ConcurrentMap<K, V> cache ;
    public LRUConcurrentCache(int maxCapacity){
        this.cache = new ConcurrentLinkedHashMap.Builder<K, V>()
                        .maximumWeightedCapacity(maxCapacity)
                        .build();
    }

    public V get(K k){
        return this.cache.get(k);

    }

    public void put(K k, V v){
        this.cache.put(k, v);
    }
}
