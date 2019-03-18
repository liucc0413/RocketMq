package rocketmq.com.example.rocketmq_producer.Service;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ProducterService {
    @Value("${rocketmq.producer.namesrvAddr}")
    private String nameAddr;
    @Value("${rocketmq.producer.groupName}")
    private String group;

    private  DefaultMQProducer defaultMQProducer;

    @PostConstruct
    public void getProducer() {
        {
            defaultMQProducer = new DefaultMQProducer(group);
            defaultMQProducer.setNamesrvAddr(nameAddr);
        }
    }

    /**
     * 同步发送消息
     * */
    public void defaultMQProducer() {

        try {
            defaultMQProducer.start();
            for(int i = 0;i < 10; i++) {

                Message message = new Message("topic-one","test","key"+i, ("sync"+i).getBytes());
//                message.setDelayTimeLevel(3);//延迟发送
                SendResult result = defaultMQProducer.send(message);
                System.out.println("send msgId " + result.getMsgId() + " resultStatus:" + result.getSendStatus());
            }
            defaultMQProducer.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 单向发送消息
     * */
    public void defaultOneWayMQProducer() {
        try {
            defaultMQProducer.start();
            for(int i = 0;i < 10; i++) {
                Message message = new Message("topic-one","test","key"+i, ("sync"+i).getBytes());
                defaultMQProducer.sendOneway(message);
            }
            defaultMQProducer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送消息
     * */
    public void asyncMqProducer() throws MQClientException, RemotingException, InterruptedException {


        defaultMQProducer.start();
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(0);

        for(int i=0;i<10;i++) {

            Message message = new Message("topic-one","test","OrderID"+i,("async"+i).getBytes());
            System.out.println(Thread.currentThread().getName() +"  product ready to send message "+ i);
            defaultMQProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(Thread.currentThread().getName() + "  async success msg:"+sendResult.getSendStatus());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("async failed msg:"+e.getMessage());
                }
            });
        }
        Thread.sleep(10000);
        defaultMQProducer.shutdown();

    }

    /**
     * 顺序消息
     * 通过轮询所有队列来发送的（负载均衡策略）
     * 消费端使用MessageListenerConcurrently
     * */
    public void orderMqProducer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        defaultMQProducer.start();
        for (int i = 1; i< 11; i++) {
            int orderId = i % 2;
            Message message = new Message("topic-one", "test","OrderID"+i,("orderMqProducer"+i).getBytes());
            System.out.println("orderMqProducer ready to send message "+ i);
            SendResult result = defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);

                }
            },1);
            System.out.println("orderMqProducer send msgId " + result.getMsgId() + " resultStatus:" + result.getSendStatus());
        }
        defaultMQProducer.shutdown();
    }

    /**
     * 批量发送的要求
     * 1.同一批次的信息要有同样的topic
     * 2.同样的waitStoreMsgOK
     * 3.不是计划消息
     * 4.同一批次所有的消息大小不超过1M
     * */
    public void BatchMqProducer(){
        try {
            defaultMQProducer.start();
            AtomicInteger integer = new AtomicInteger();
            System.out.println(integer.getAndIncrement());
            List<Message> messages = new ArrayList<>();
            for (int i = 1; i< 2; i++) {
                Message message = new Message("topic-one", "test","OrderID"+i,("BatchMqProducer"+i).getBytes());
                System.out.println("BatchMqProducer ready to send message "+ i);
                messages.add(message);
            }
            SendResult result = defaultMQProducer.send(messages);
            System.out.println( "BatchMqProducer resultStatus:" + result.getSendStatus());
            defaultMQProducer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 事务消息
     * */
    public void DefultTransactionProducer() throws MQClientException, InterruptedException {
        TransactionMQProducer producer = new TransactionMQProducer(group);
        producer.setNamesrvAddr(nameAddr);

        TransactionListener listener = new TransactionListenerImpl();
        ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("transaction-thread");
                return thread;
            }
        });

        producer.setExecutorService(executorService);
        producer.setTransactionListener(listener);
        producer.start();

        for (int i = 0; i< 10; i++) {
            Message message = new Message("topic-one", "TagA","OrderID"+i,("DefultTransactionProducer"+i).getBytes());
            TransactionSendResult result = producer.sendMessageInTransaction(message,null);
            Thread.sleep(10);
        }

        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();

    }

    public void test() throws InterruptedException, MQClientException {
       TransactionListener transactionListener = new TransactionListenerImpl();
       TransactionMQProducer producer = new TransactionMQProducer(group);
       producer.setNamesrvAddr(nameAddr);
       ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
           @Override
           public Thread newThread(Runnable r) {
               Thread thread = new Thread(r);
               thread.setName("client-transaction-msg-check-thread");
               return thread;
           }
       });

       producer.setExecutorService(executorService);
       producer.setTransactionListener(transactionListener);
       producer.start();

       String[] tags = new String[] {"TagA", "TagB"};
       for (int i = 0; i < 10; i++) {
           try {
               Message msg =
                       new Message("TopicTest1234", tags[i % tags.length], "KEY" + i,
                               ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
               SendResult sendResult = producer.sendMessageInTransaction(msg, null);
//               System.out.printf("%s%n", sendResult);

               Thread.sleep(10);
           } catch (MQClientException | UnsupportedEncodingException e) {
               e.printStackTrace();
           }
       }

       for (int i = 0; i < 100; i++) {
           Thread.sleep(1000);
       }
       producer.shutdown();


   }
}
