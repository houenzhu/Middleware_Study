package com.zhe.utils;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @version 1.0
 * @Author 朱厚恩
 * rabbitmq连接工具类
 */

@Slf4j
public class RabbitMQConnectionUtil {
    private static final String HOST = "192.168.198.128";
    private static final int PORT = 5672;
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin123";
    private static final String VIRTUAL_HOST = "/";



    /**
     * 获取rabbitmq连接
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public static Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);

        // 设置超时设置
        factory.setConnectionTimeout(30000);
        // 设置心跳检测间隙
        factory.setRequestedHeartbeat(60);
        // 自动恢复
        factory.setAutomaticRecoveryEnabled(true);

        return factory.newConnection();
    }

    public static void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (IOException e) {
            System.err.println("关闭 RabbitMQ 异常: " + e.getMessage());

        }
    }
}
