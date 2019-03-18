package rocketmq.com.example.rocketmq_consume;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@SpringBootApplication
public class RocketmqConsumeApplication {

    public static void main(String[] args) {
        SpringApplication.run(RocketmqConsumeApplication.class, args);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("lcc-one");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setInstanceName("rmq-instance");
        try {
            consumer.subscribe("topic-one","test");
            System.out.println("****************************");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg: msgs) {
                    System.out.println("consumer " + msg.getBody());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }

}
