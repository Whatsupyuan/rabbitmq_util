package com.captain.utils.mq.rabbitmq;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;

public class RabbitmqUtilTest {

    @Test
    public void test01_send() {
        Connection connection = null;
        int size = 1000;
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            Channel channel = connection.createChannel();
            for (int i = 0; i < size; i++) {
                RabbitmqUtil.send(channel, "TEST_QUEUE_02", "test Message" + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //RabbitmqUtil.closeConnection(connection);
        }
    }

    @Test
    public void test02_receive_bySize() {
        Connection connection = null;
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            LinkedList<String> list = RabbitmqUtil.receiveBySize(connection, "TEST_QUEUE_02", 10, "utf-8", true);
//            for (String str : list) {
//                System.out.println(str);
//            }
            System.out.println(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RabbitmqUtil.closeConnection(connection);
        }
    }

    @Test
    public void test_receive_consumer01() {
        Connection connection = null;
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            RabbitmqUtil.receiveAll(connection, "TEST_QUEUE_02", "utf-8", false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //RabbitmqUtil.closeConnection(connection);
        }
    }


    @Test
    public void test_receive_consumer02() {
        Connection connection = null;
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            RabbitmqUtil.receiveAll(connection, "TEST_QUEUE_02", "utf-8", false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RabbitmqUtil.closeConnection(connection);
        }
    }


    @Test
    public void consumer_03() {
        try {
            //定义连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            //设置服务地址
            factory.setHost("192.168.8.47");
            //端口
            factory.setPort(5672);
            //设置账号信息，用户名、密码、vhost
            factory.setVirtualHost("testhost".trim().startsWith("/") ? "testhost" : "/" + "testhost");
            factory.setUsername("hryuan");
            factory.setPassword("admin");
            // 通过工程获取连接
            Connection connection = factory.newConnection();

            final Channel channel = connection.createChannel();
            //channel.basicQos(1);
            channel.basicConsume("TEST_QUEUE_02", false, "",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body)
                                throws IOException {
                            String routingKey = envelope.getRoutingKey();
                            String contentType = properties.getContentType();
                            long deliveryTag = envelope.getDeliveryTag();
                            System.out.println(consumerTag + "- " + routingKey + " - " +
                                    contentType + " - " + deliveryTag + " - " + new String(body));
                            channel.basicAck(deliveryTag, false);
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void consumer_04() {
        Connection connection = null;
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            RabbitmqUtil.receiveAll_noIntercepter(connection, "TEST_QUEUE_02", 10, "utf-8", false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RabbitmqUtil.closeConnection(connection);
        }
    }

    @Test
    public void consumer() {
        try {
            Connection connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
            RabbitmqUtil.receiveAll_20190806(connection , "TEST_QUEUE_02" , "utf-8" );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




}