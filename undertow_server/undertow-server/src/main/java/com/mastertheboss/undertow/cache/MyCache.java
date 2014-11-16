package com.mastertheboss.undertow.cache;


public interface MyCache<K, V>{
    public V get(K key);
    public void put(K key, V value);
}
