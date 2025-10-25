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

public class WorkQueueConsumer {
    private static final String QUEUE_NAME = "work.queue";

    private String consumerName;
    private Connection connection;
    private Channel channel;
    private ObjectMapper objectMapper;

    public WorkQueueConsumer(String consumerName) throws IOException, TimeoutException {
        this.consumerName = consumerName;
        connection = RabbitMQConnectionUtil.getConnection();
        channel = connection.createChannel();
        objectMapper = new ObjectMapper();

        // 声明工作队列
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // 设置 QoS：公平分发，每次只处理一条消息
        channel.basicQos(1);
        System.out.println(consumerName + " 初始化完成，等待接收消息...");
    }

    /**
     * 开始消费工作队列消息
     */
    public void startConsuming() throws IOException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String messageJson = new String(consumerTag.getBytes(), StandardCharsets.UTF_8);
                NotificationMessage message = objectMapper.readValue(messageJson, NotificationMessage.class);
                System.out.println(consumerName + " 开始处理消息: " + message.getTitle());
                // 模拟处理时间
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                processWorkMessage(message);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                System.err.println(consumerName + " 处理消息失败: " + e.getMessage());
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
    }

    private void processWorkMessage(NotificationMessage message) {
        // 工作队列的处理逻辑
        System.out.println(consumerName + " 正在处理: " + message.getContent());
    }

    public void close() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
        System.out.println(consumerName + " 连接已关闭");
    }
}
