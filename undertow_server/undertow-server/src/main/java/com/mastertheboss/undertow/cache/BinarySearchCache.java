package com.mastertheboss.undertow.cache;

import java.util.Comparator;
import java.util.Arrays;

public class BinarySearchCache<K, V> implements MyCache<K, V>{

    K[] keyArray;
    V[] valueArray;
    Comparator<? super K> c;

    public BinarySearchCache(){

    }

    public BinarySearchCache(K[] sortedKeyArray, V[] sortedValueArray,  Comparator<? super K> c){
        this.set(sortedKeyArray, sortedValueArray, c);
    }

    public void set(K[] sortedKeyArray, V[] sortedValueArray,  Comparator<? super K> c){
        this.keyArray = sortedKeyArray;
        this.valueArray = sortedValueArray;
        this.c = c;
    }

    public V get(K key){
        int i = Arrays.binarySearch(keyArray, key, c);
        if(i < 0){return null;}
        return valueArray[i];
    }
    public void put(K key, V value){
        // throw away
        return ;
    }

}
