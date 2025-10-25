package com.zhe.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.zhe.mq.entity.NotificationMessage;
import com.zhe.utils.RabbitMQConnectionUtil;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @version 1.0
 * @Author 朱厚恩
 */

public class TopicExchangeExample {
    private static final String TOPIC_EXCHANGE_NAME = "notification.topic.exchange";
    private Connection connection;
    private Channel channel;
    private ObjectMapper objectMapper;

    public TopicExchangeExample() throws IOException, TimeoutException {
        connection = RabbitMQConnectionUtil.getConnection();
        channel = connection.createChannel();
        objectMapper = new ObjectMapper();
        // 声明主题交换机
        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true, false, null);
    }

    /**
     * 发送主题消息
     * 路由间格式: notification.type.action
     * 例如: notification.system.create, notification.user.update
     */
    public void sendTopMessage(NotificationMessage message, String routingKey) throws IOException {
        message.setId(UUID.randomUUID().toString().replaceAll("-", ""));
        String messageJson = objectMapper.writeValueAsString(message);
        channel.basicPublish(TOPIC_EXCHANGE_NAME, routingKey, null, messageJson.getBytes());
        System.out.println("发送主题消息: [" + routingKey + "]: " + message.getTitle());
    }

    /**
     * 绑定队列到主题交换机
     */
    public void bindQueueToTopic(String queueName, String routingKey) throws IOException {
        // 声明队列
        channel.queueDeclare(queueName, true, false, false, null);
        // 绑定队列到主题交换机
        channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, routingKey);

        System.out.println("绑定队列: " + queueName + " 到主题交换机: " + TOPIC_EXCHANGE_NAME + "，路由键: " + routingKey);
    }

    public void close() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }
}
