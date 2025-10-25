package com.zhe.mq.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.zhe.mq.entity.NotificationMessage;
import com.zhe.utils.RabbitMQConnectionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @version 1.0
 * @Author 朱厚恩
 * 消息生产者
 */

public class NotificationProducer {
    // 交换机名称
    private static final String EXCHANGE_NAME = "notification.exchange";
    // 队列名称
    private static final String QUEUE_NAME = "notification.queue";
    // 路由键
    private static final String ROUTING_KEY = "notification.routing.key";
    private final Connection connection;
    private final Channel channel;
    private final ObjectMapper objectMapper;

    public NotificationProducer() {
        try {
            connection = RabbitMQConnectionUtil.getConnection();
            channel = connection.createChannel();
            objectMapper = new ObjectMapper();

            // 声明直连交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

            // 声明队列
            // durable: true 持久化
            // exclusive: false 非独占
            // autoDelete: false 不自动删除
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // 绑定队列到交换机
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            System.out.println("生产者初始化完成...");
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendNotification(NotificationMessage message) {
        // 生成消息ID
        message.setId(UUID.randomUUID().toString().replaceAll("-", ""));
        try {
            String messageJson = objectMapper.writeValueAsString(message);
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, messageJson.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("发送消息: " + message.getTitle() + " | 用户ID: " + message.getUserId());
    }

    public void sendBatchNotifications(List<NotificationMessage> messages) {
        messages.forEach(this::sendNotification);
    }

    /**
     * 发送延迟消息(死信队列)
     */
    public void sendDelayedNotification(NotificationMessage message, long delayMillis) throws IOException {
        String delayExchange = "delayed.notification.exchange";
        String delayQueue = "delayed.notification.queue";
        // 1. 声明延迟队列参数
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        try {
            channel.exchangeDeclare(delayExchange, "x-delayed-message", true, false, args);
        } catch (IOException e) {
            // 交换机可能已存在，忽略错误或记录日志
            System.out.println("延迟交换机可能已存在: " + e.getMessage());
        }

        // 2. 声明延迟队列
        channel.queueDeclare(delayQueue, true, false, false, args);

        // 3. 绑定队列到延迟交换机
        channel.queueBind(delayQueue, delayExchange, ROUTING_KEY);
        message.setId(UUID.randomUUID().toString().replaceAll("-", ""));
        String messageJson = objectMapper.writeValueAsString(message);
        // 4. 设置延迟消息头
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-delay", delayMillis);
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.headers(headers);
        // 5. 发送到延迟交换机
        channel.basicPublish(delayExchange, ROUTING_KEY, builder.build(), messageJson.getBytes());
        System.out.println("发送延迟消息: " + message.getTitle() + " | 用户ID: " + message.getUserId() + " | 延迟时间: " + delayMillis + "毫秒");
    }

    /**
     * 关闭连接
     */
    public void close() throws IOException, TimeoutException {
        if (channel.isOpen()) {
            channel.close();
        }
        if (connection.isOpen()) {
            connection.close();
        }
        System.out.println("生产者连接已关闭");
    }
}
