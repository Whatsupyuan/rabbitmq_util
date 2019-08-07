package com.captain.utils.mq.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rabbitmq
 * <p>
 * 生产 & 消费 工具类
 */
public class ExchangeUtil {

    private static Logger logger = LoggerFactory.getLogger(ExchangeUtil.class);

    /**
     * 生产
     *
     * @param exchangeName
     * @param routingKey
     * @param message
     * @throws Exception
     */
    public static void producer(Channel channel, String exchangeName,
                                String routingKey,
                                String message) throws Exception {
        try {
            // 发送消息
            channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
            logger.info("-- 发送完成 --");
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 消费
     *
     * @param connection
     * @param exchangeName
     * @param routingKey
     */
    public static void consumer(BuiltinExchangeType builtinExchangeType,
                                Connection connection, String exchangeName,
                                String routingKey, String queueName,
                                boolean autoAck, String encoding, ProcessingInterface<Boolean> processing) throws Exception {
        Channel channel = connection.createChannel();
        encoding = StringUtils.isNotBlank(encoding) ? encoding : "utf-8";
        try {
            /** 声明交换机
             * durable 是否进行持久化 (Durability:是否需要持久化，true为持久化)
             * autoDelete (Auto Delete：当最后一个绑定到exchange上的队列删除后，自定删除该exchange)
             * Internal 当前exchange是否用于rabbitmq内部使用，默认为false
             *
             * 注 : 消息发送到没有队列绑定的交换机时，消息将丢失，因为，交换机没有存储消息的能力，消息只能存在在队列中 */
            channel.exchangeDeclare(exchangeName, builtinExchangeType.getType(), true, false, false, null);
            // 声明队列
            channel.queueDeclare(queueName, false, false, false, null);

            // 按照每个消费者的能力分配消息呢
            // prefetchCount 设为1时，队列只有在收到消费者发回的上一条消息 ack 确认后，才会向该消费者发送下一条消息。
            // prefetchCount 的默认值为0，即没有限制，队列会将所有消息尽快发给消费者。
            channel.basicQos(1);

            // 如果交换机的注册模式为 FANOUT , 则不需要 routingKey
            routingKey = builtinExchangeType.getType().equals(BuiltinExchangeType.FANOUT.getType()) ? "" : routingKey;

            // 建立绑定关系 START
            channel.queueBind(queueName, exchangeName, routingKey);
            while (true) {
                GetResponse response = channel.basicGet(queueName, autoAck);
                if (response == null) {
                } else {

                    // [processing program] START
                    logger.info(new String(response.getBody(), encoding));
                    // 处理
                    if (processing.processing(new String(response.getBody(), encoding))) {
                        // 手动确认
                        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    } else {
                        // 消息处理失败 , 重回队列
                        /** 参数：*  long deliveryTag：消息标签*
                         * boolean multiple：是否批量*
                         * boolean requeue：是否重回队列*  如果设置成true,那么失败的消息会重新放到消息的最后
                         * */
                        channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                    }
                    // [processing program] END

                }
            }
        } catch (Exception e) {
            throw e;
        } finally {
            channel.close();
        }
    }

}
