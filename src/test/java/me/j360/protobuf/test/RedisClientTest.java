package me.j360.protobuf.test;

import me.j360.protobuf.PersonModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Package: me.j360.protobuf.test
 * User: min_xu
 * Date: 16/6/20 下午4:49
 * 说明：
 */
public class RedisClientTest {
    private Jedis jedis;

    @Before
    public void setup() {
        jedis = new Jedis("123.59.27.174", 6379);
    }

    /**
     * redis存储字符串
     */
    @Test
    public void testJedisProtobuf() throws IOException {

        PersonModel.Person person = PersonModel.Person.newBuilder()
                .setId(11).setName("xumin")
                .setEmail("j360.me").build();
        System.out.println(person);
        Assert.assertEquals(11, person.getId());

        // 将数据写到输出流，如网络输出流，这里就用ByteArrayOutputStream来代替
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        person.writeTo(output);

        // -------------- 分割线：上面是发送方，将数据序列化后发送 ---------------
        String key = "test001";
        jedis.set(key.getBytes(),person.toByteArray());
        jedis.expire(key,10);

        String sss = jedis.get(key);
        System.out.println(sss);


    }

}
