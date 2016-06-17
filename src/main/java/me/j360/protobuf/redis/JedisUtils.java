package me.j360.protobuf.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class JedisUtils {

    private static final String OK_STATUS = "OK";


    private static final String OK_MULTI_STATUS = "+OK";


    /**
     * 强行关闭Pool以外的连接
     * @param jedis
     */
    public static void destroyJedis(Jedis jedis){
        if((jedis!=null) && jedis.isConnected()){
            jedis.quit();
            jedis.disconnect();
        }
    }

    public static boolean ping(JedisPool jedisPool){
        JedisTemplate jedisTemplate = new JedisTemplate(jedisPool);
        try {
            String result = jedisTemplate.execute(new JedisTemplate.JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    return jedis.ping();
                }
            });
            return (result != null) && result.equals("PONG");
        }catch (JedisException e){
 //           log.error("ping the redis server has a exceptino {}",e.getMessage());
            return false;
        }
    }

    public static Boolean isStatusOk(String status) {

        return (status!=null) && (OK_STATUS.equals(status) || OK_MULTI_STATUS.equals(status));
    }
}
