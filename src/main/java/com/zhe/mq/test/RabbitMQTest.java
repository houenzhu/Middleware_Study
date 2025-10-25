package com.zhe.mq.test;

import com.zhe.mq.consumer.NotificationConsumer;
import com.zhe.mq.entity.NotificationMessage;
import com.zhe.mq.producer.NotificationProducer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @version 1.0
 * @Author 朱厚恩
 */

public class RabbitMQTest {
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        testBasicProducerConsumer();
    }

    /**
     * 测试基本的生产者和消费者
     */
    public static void testBasicProducerConsumer() throws InterruptedException, IOException, TimeoutException {

        // 启动消费者
        new Thread(() -> {
            try {
                NotificationConsumer consumer = new NotificationConsumer();
                consumer.startConsumingManualAck();
                Thread.sleep(30000);
                consumer.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        // 等待消费者启动
        Thread.sleep(2000);
        NotificationProducer producer = new NotificationProducer();
        // 发送多条测试消息
        for (int i = 1; i <= 5; i++) {
            NotificationMessage message = new NotificationMessage(
                    1000L + i,
                    "系统通知 " + i,
                    "这是第 " + i + " 条测试消息内容",
                    "系统管理员"
            );
            message.setType(1);
            producer.sendNotification(message);
            Thread.sleep(1000);
        }

        // 发送延迟消息测试
        NotificationMessage delayedMessage = new NotificationMessage(
                2000L,
                "延迟通知",
                "这是一条延迟5秒的消息",
                "定时任务"
        );
        producer.sendDelayedNotification(delayedMessage, 5000);
        Thread.sleep(10000);
        producer.close();
        System.out.println("基本生产消费测试完成");
    }
}
