package com.mastertheboss.undertow.memcached;
import net.rubyeye.xmemcached.*;
import net.rubyeye.xmemcached.utils.AddrUtil;

public class MemcachedInitiator{

    public static MemcachedClient getClient(String addr , int poolSize) throws Exception{
        // addr (e.g. "localhost:12000")
        MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(addr));
        builder.setConnectionPoolSize(poolSize); //set connection pool size to five
        MemcachedClient memcachedClient = builder.build();
        return memcachedClient;
    }
}
