package com.captain.utils.mq.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ExchangeUtilTest {

    String exchangeName = "fanout_exchange01";

    String exchangeRoutingKey = "fanout_routing_key";
    String exchangeRoutingKey_01 = "fanout_routing_key_01";

    String queueName1 = "queue_4_fanout_03";
    String queueName2 = "queue_4_fanout_04";

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
            ExchangeUtil.producer(connection.createChannel(), exchangeName,
                    exchangeRoutingKey_01, "admin222");
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

    /*@Test
    public void consumer_1() {
        try {
            ExchangeUtil.consumer(BuiltinExchangeType.DIRECT, connection, exchangeName, exchangeRoutingKey,
                    queueName1, false, "utf-8", () -> {
                        return true;
                    });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }*/

    /*@Test
    public void consumer_2() {
        try {
            ExchangeUtil.consumer(BuiltinExchangeType.DIRECT, connection, exchangeName, exchangeRoutingKey_01,
                    queueName2, false, "utf-8", () -> {
                        return true;
                    });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }*/

}