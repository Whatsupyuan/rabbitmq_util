package com.captain.utils.mq.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

/**
 * Rabbitmq 操作工具类
 *
 * @param
 * @author hryuan911
 * @date 2019/8/2
 * @return
 */
public class RabbitmqUtil {

    /**
     * 获取 connection
     *
     * @param host
     * @param username
     * @param passwd
     * @param port
     * @param virtualHost
     * @return
     * @throws Exception
     */
    public static Connection getConnection(String host, String username,
                                           String passwd, int port,
                                           String virtualHost) throws Exception {
        //定义连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        //设置服务地址
        factory.setHost(host.trim());
        //端口
        factory.setPort(port);
        //设置账号信息，用户名、密码、vhost
        factory.setVirtualHost(virtualHost.trim().startsWith("/") ? virtualHost : "/" + virtualHost);
        factory.setUsername(username.trim());
        factory.setPassword(passwd.trim());
        // 通过工程获取连接
        Connection connection = factory.newConnection();
        return connection;
    }


    /**
     * 发送内容到队列中
     *
     * @param queueName
     * @param message
     */
    public static void send(Channel channel, String queueName, String message)
            throws Exception {
        // 声明（创建）队列
        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicPublish("", queueName, null, message.getBytes());
    }

    /**
     * 从队列中获取数据
     *
     * @param connection 连接
     * @param queueName
     * @param encoding   编码格式
     * @param autoAck    确认是否从队列中删除 true 删除 / false 不删除     * @return java.util.LinkedList<java.lang.String>
     * @author hryuan911
     * @date 2019/8/2
     */
    @Deprecated
    public static void receiveAll(Connection connection, String queueName, String encoding, boolean autoAck) {
        // 从连接中创建通道
        final String coding = StringUtils.isNotBlank(encoding) ? encoding : "UTF-8";
        try {

            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            final Channel channel = connection.createChannel();
            //channel.basicQos(1);
            channel.basicConsume(queueName, autoAck, "",
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    @Deprecated
    public static void receiveAll_20190806(Connection connection, String queue, String encoding) {
        try {
            //connection = ApplicationListener.rabitmqFactory.newConnection();
            //创建一个通道
            Channel channel = connection.createChannel();
            //channel.queueBind("test","testExchange","testRouting");
            channel.queueDeclare(queue, false, false, false, null);
            //channel.exchangeDeclare("testExchange", BuiltinExchangeType.DIRECT,true,false,null);

            //声明要关注的队列
            //DefaultConsumer类实现了Consumer接口，通过传入一个频道，
            // 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    System.out.println(new String(body, encoding));
                    //channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };
            //自动回复队列应答 -- RabbitMQ中的消息确认机制
            channel.basicConsume(queue, true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 从队列中获取数据
     *
     * @param connection 连接
     * @param queueName
     * @param size       需要消费的数量
     * @param encoding   编码格式
     * @param autoAck    确认是否从队列中删除 true 删除 / false 不删除
     * @return https://www.rabbitmq.com/api-guide.html
     * [Retrieving Individual Messages ("Pull API")]
     */
    public static LinkedList<String> receiveBySize(Connection connection, String queueName, int size,
                                                   String encoding, boolean autoAck) {
        LinkedList<String> list = new LinkedList<>();
        // 从连接中创建通道
        Channel channel = null;
        encoding = StringUtils.isNotBlank(encoding) ? encoding : "UTF-8";
        try {
            channel = connection.createChannel();
            // 声明队列
            for (int i = 0; i < size; i++) {
                GetResponse response = channel.basicGet(queueName, autoAck);
                if (response == null) {
                    // No message retrieved.
                } else {
                    list.add(new String(response.getBody(), encoding));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * [持续监听] 消费所有内容 , 无阻断
     *
     * @param connection 连接
     * @param queueName
     * @param size       需要消费的数量
     * @param encoding   编码格式
     * @param autoAck    确认是否从队列中删除 true 删除 / false 不删除
     * @return
     */
    public static void receiveAll_noIntercepter(Connection connection, String queueName, int size,
                                                String encoding, boolean autoAck) {
        // 从连接中创建通道
        Channel channel = null;
        encoding = StringUtils.isNotBlank(encoding) ? encoding : "UTF-8";
        try {
            channel = connection.createChannel();
            // 声明队列
            while (true) {
                GetResponse response = channel.basicGet(queueName, autoAck);
                if (response == null) {
                    // No message retrieved.
                } else {
                    // (processing program) start
                    System.out.println(new String(response.getBody(), encoding));
                    // (processing program) end

                    // ack模式: autoAck 为 false 则需要手动确认
                    if (!autoAck) {
                        // 第二个参数为 : 是否批量处理, 为true时则将 deliveryTag 小于当前 tag 的全部修改为以确认
                        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭 连接
     *
     * @param connection
     */
    public static void closeConnection(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭 通道
     *
     * @param channel
     */
    public static void closeChannel(Channel channel) {
        if (null != channel) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }


}
