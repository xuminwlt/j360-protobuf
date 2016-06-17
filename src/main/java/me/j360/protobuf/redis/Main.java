package me.j360.protobuf.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Peter on 15-4-22.
 */
public class Main {

    public static void main(String[] args) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(2);
        config.setMaxWaitMillis(3000000);
//        JedisPool jedisPool = new JedisDirectPool("test",new HostAndPort("test.fotoplace.cc",6379),config);
//        JedisTemplate jedisTemplate = new JedisTemplate(jedisPool);
//        String value = jedisTemplate.get("test");
//        jedisTemplate.setex("hello","fotoplace",10);
//        System.out.println(value);
        HostAndPort[] hostAndPorts = new HostAndPort[]{new HostAndPort("123.59.13.114",12580)};
        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("test",hostAndPorts,"masterNode01",config);
        final JedisTemplate jedisTemplate = new JedisTemplate(jedisSentinelPool);

        String key = "";
        byte[] value = null;
        jedisTemplate.set(key.getBytes(),value);

    }
}
