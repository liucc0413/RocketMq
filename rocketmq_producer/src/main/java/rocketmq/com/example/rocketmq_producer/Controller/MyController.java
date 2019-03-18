package rocketmq.com.example.rocketmq_producer.Controller;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import rocketmq.com.example.rocketmq_producer.Service.ProducterService;

@Controller
@RequestMapping("/mq")
public class MyController {
    @Autowired
    ProducterService producterService;



    @RequestMapping("/test1")
    @ResponseBody
    public void test1() {
        System.out.println("test1*********");
        producterService.defaultOneWayMQProducer();

    }

    @RequestMapping("/test2")
    @ResponseBody
    public void test2() {

        try {
            producterService.asyncMqProducer();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    @RequestMapping("/test3")
    @ResponseBody
    public void test3() {
        producterService.defaultOneWayMQProducer();

    }

    @RequestMapping("/test4")
    @ResponseBody
    public void test4() {
        try {
            producterService.orderMqProducer();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        }

    }

    @RequestMapping("/test5")
    @ResponseBody
    public void test5() {
        producterService.BatchMqProducer();

    }
    @RequestMapping("/test6")
    @ResponseBody
    public void test6() {
        try {
            producterService.DefultTransactionProducer();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @RequestMapping("/test7")
    @ResponseBody
    public void test７() {
        try {
            producterService.test();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
