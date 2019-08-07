package com.captain.utils.mq.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * exchange - topic
 */
public class ExchangeUtilTest01 {

    private Connection connection;

    @Before
    public void getConnection() {
        try {
            connection = RabbitmqUtil.getConnection("192.168.8.47", "hryuan", "admin", 5672, "testhost");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void product() {
        try {
            Channel channel = connection.createChannel();
            for (int i = 0; i < 10; i++) {
                ExchangeUtil.producer(channel, "topic_exchage", "nba.james", "admin_" + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void consumer01() {
        try {
            ExchangeUtil.consumer(
                    BuiltinExchangeType.TOPIC, connection, "topic_exchage", "nba.#", "topic_queue", false, "utf-8",
                    (message) -> {
                        if (message.endsWith("5")) {
                            return false;
                        } else {
                            return true;
                        }
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}