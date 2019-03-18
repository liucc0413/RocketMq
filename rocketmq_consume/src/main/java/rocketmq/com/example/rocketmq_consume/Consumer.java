package rocketmq.com.example.rocketmq_consume;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("lcc-one");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setInstanceName("rmq-instance");
        consumer.subscribe("topic-one","test");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg: msgs) {
                    if (msg.getKeys().equals("OrderID8")) {
                        throw new RuntimeException("Q@!!!!!!!!!!!!!!!!!!!!!!!!!");
                    }
                    System.out.println("consumer " + msg.getBody());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }
}
