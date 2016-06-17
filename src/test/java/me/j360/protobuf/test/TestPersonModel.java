package me.j360.protobuf.test;

import me.j360.protobuf.PersonModel;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Package: me.j360.protobuf.test
 * User: min_xu
 * Date: 16/6/17 下午5:46
 * 说明：
 */
public class TestPersonModel {

    @Test
    public void testProto() throws IOException {
        PersonModel.Person person = PersonModel.Person.newBuilder()
                .setId(100).setName("zhuliangliang")
                .setEmail("zhuliangliang.me").build();
        System.out.println(person);
        Assert.assertEquals(100, person.getId());


        // 将数据写到输出流，如网络输出流，这里就用ByteArrayOutputStream来代替
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        person.writeTo(output);

        // -------------- 分割线：上面是发送方，将数据序列化后发送 ---------------
        byte[] byteArray = output.toByteArray();

        // -------------- 分割线：下面是接收方，将数据接收后反序列化 ---------------
        // 接收到流并读取，如网络输入流，这里用ByteArrayInputStream来代替
        ByteArrayInputStream input = new ByteArrayInputStream(byteArray);
        // 反序列化
        PersonModel.Person xxg2 = PersonModel.Person.parseFrom(input);
        System.out.println("ID:" + xxg2.getId());
        System.out.println("name:" + xxg2.getName());
        System.out.println("email:" + xxg2.getEmail());
    }

}
