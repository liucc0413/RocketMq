package rocketmq.com.example.rocketmq_producer.Service;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class ConsumerService {

    @Value("${rocketmq.producer.namesrvAddr}")
    private String nameAddr;
    @Value("${rocketmq.producer.groupName}")
    private String group;

   /**
    * 无序消费
    * 和defultConsumer2的conusmer group,topic和tag一致是同一个集群
    * */
    @PostConstruct
    public void defultConsumer() throws MQClientException {
        int max = 2;
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(nameAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("topic-one","test");
        consumer.setInstanceName("one");
        /*设置消费失败后的重试次数，重试指定次数还是失败后不成功，则被放入死信队列，需要人工干预*/
        consumer.setMaxReconsumeTimes(max);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg: msgs) {
                    try {
                        try {
                            System.out.println("******消费 One:" + new String(msg.getBody(), "utf-8") + " key is:" +msg.getKeys() +" tag:"+msg.getTags() +" topic:"+msg.getTopic() + " thread:"+Thread.currentThread().getName() );
                            Thread.sleep(1000*10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }finally {
                        if(msg.getReconsumeTimes() == max) {
                            System.out.println("到了最大重试次数了");
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }

    /**
     * 无序消费
     * 和defultConsumer1的conusmer group,topic和tag一致是同一个集群
     * */
//   @PostConstruct
    public void defultConsumer2() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(nameAddr);
        consumer.subscribe("topic-one","test");
        consumer.setInstanceName("two");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg: msgs) {
                    try {
                        System.out.println("******消费 Two:" + new String(msg.getBody(), "utf-8") + " key is:" +msg.getKeys());
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }

    /**
     * 顺序消费
     * 同一个队列里的信息顺序消费
     * 只是针对集群消费
     * */
//    @PostConstruct
    public void defultConsumerOrderly() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("multi-cloud");
        consumer.setNamesrvAddr(nameAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest1234","TagA||TagB");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg: msgs) {
                    try {
                        System.out.println("###defultConsumerOrderly msg:" + new String(msg.getBody(), "utf-8") + " key is:" +msg.getKeys());
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();

    }
}
