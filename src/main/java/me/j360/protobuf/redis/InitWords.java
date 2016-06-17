package me.j360.protobuf.redis;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by Peter on 15-4-22.
 */
public class InitWords {

    public static void main(String[] args) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(2);
        config.setMaxWaitMillis(3000000);

        HostAndPort[] hostAndPorts = new HostAndPort[]{new HostAndPort("123.59.13.114",12580)};
        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("test",hostAndPorts,"masterNode01",config);
        final JedisTemplate jedisTemplate = new JedisTemplate(jedisSentinelPool);
        try {
            List<String> list = Files.readLines(new File("/Users/Peter/newcode/app/app-web/target/app-web/WEB-INF/sensor_words.txt"), Charsets.UTF_8);
            jedisTemplate.sadd("sensorWorlds",list.toArray(new String[list.size()]));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
