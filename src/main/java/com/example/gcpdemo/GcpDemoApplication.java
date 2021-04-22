package com.example.gcpdemo;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.core.PubSubOperations;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class GcpDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(GcpDemoApplication.class, args);
  }

  // Bean definition for supporting daemon process
  @Bean
  ThreadPoolTaskScheduler pubsubSubscriberThreadPool() {
    return new ThreadPoolTaskScheduler();
  }

  @Component
  static class MyApplicationRunner implements ApplicationRunner {

    @Autowired
    MyService service;

    @Autowired
    PubSubOperations pubSubOperations;

    @Override
    public void run(ApplicationArguments args) {
      String subscriptionName = "someTopic-sub";
      Subscriber subscriber = pubSubOperations.subscribe(subscriptionName, message -> {
        service.execute(message);
        message.ack();
      });
    }

  }

  @Service
  static class MyService {
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);
    private final BlockingQueue<BasicAcknowledgeablePubsubMessage> messages = new LinkedBlockingQueue<>();

    BasicAcknowledgeablePubsubMessage pollMessage() throws InterruptedException {
      return messages.poll(5, TimeUnit.SECONDS);
    }

    @Autowired
    MyMapper myMapper;

    @Transactional
    public void execute(BasicAcknowledgeablePubsubMessage message) {
      logger.info("Message received subscription: "
          + message.getPubsubMessage().getData().toStringUtf8() + " attributes:"
          + message.getPubsubMessage().getAttributesMap());
      messages.add(message);
      logger.info("Start.");
      logger.info("Ping {}.", myMapper.ping());
      logger.info("End.");
    }
  }

  @Mapper
  interface MyMapper {
    @Select("select 1")
    int ping();
  }

}
