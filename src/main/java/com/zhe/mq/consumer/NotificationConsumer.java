package com.zhe.mq.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.zhe.mq.entity.NotificationMessage;
import com.zhe.utils.RabbitMQConnectionUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @version 1.0
 * @Author 朱厚恩
 */

public class NotificationConsumer {
    // 交换机名称
    private static final String EXCHANGE_NAME = "notification.exchange";
    // 队列名称
    private static final String QUEUE_NAME = "notification.queue";
    // 路由键
    private static final String ROUTING_KEY = "notification.routing.key";

    private Connection connection;
    private Channel channel;
    private ObjectMapper object_mapper;

    public NotificationConsumer() {
        try {
            connection = RabbitMQConnectionUtil.getConnection();
            channel = connection.createChannel();
            object_mapper = new ObjectMapper();

            // 声明直连交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

            // 声明队列
            // durable: true 持久化
            // exclusive: false 非独占
            // autoDelete: false 不自动删除
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // 绑定队列到交换机
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            // 设置 QoS：每次只处理一条消息，处理完再获取下一条
            channel.basicQos(1);
            System.out.println("消费者初始化完成，等待接收消息...");
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 开始消费消息（自动确认模式）
     */
    public void startConsumingAutoAck() throws IOException {

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String messageJson = new String(delivery.getBody(), StandardCharsets.UTF_8);
                NotificationMessage message = object_mapper.readValue(messageJson, NotificationMessage.class);

                // 处理消息
                processMessage(message);
                System.out.println("消息处理完毕，准备接收下一条消息...");
            } catch (Exception e) {
                System.err.println("处理消息异常: " + e.getMessage());
            }
        };

        // 开始消费消息
        // autoAck: true 自动确认消息
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
    }

    /**
     * 开始消费消息（手动确认模式）
     */
    public void startConsumingManualAck() throws IOException {
        System.out.println("消费者初始化完成，等待接收消息...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String messageJson = new String(delivery.getBody(), StandardCharsets.UTF_8);
                NotificationMessage message = object_mapper.readValue(messageJson, NotificationMessage.class);

                processMessage(message);

                // 手动确认消息
                // multiple: false 不批量确认
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.err.println("处理消息异常: " + e.getMessage());

                // 处理失败，拒绝消息
                // requeue: true 重新放回队列
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        };

        // 开始消费消息
        // autoAck: false 手动确认消息
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    /**
     * 处理消息的业务逻辑
     */
    private void processMessage(NotificationMessage message) {
        // 模拟业务处理
        System.out.println("=== 处理通知消息 ===");
        System.out.println("消息ID: " + message.getId());
        System.out.println("用户ID: " + message.getUserId());
        System.out.println("标题: " + message.getTitle());
        System.out.println("内容: " + message.getContent());
        System.out.println("发送者: " + message.getSender());
        System.out.println("时间: " + message.getCreateTime());
        System.out.println("=========================");

        // 这里可以添加实际的业务逻辑，比如：
        // 1. 保存到数据库
        // 2. 调用其他服务
        // 3. 发送邮件或短信
        // 4. 推送 WebSocket 消息

        // 模拟处理时间
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
