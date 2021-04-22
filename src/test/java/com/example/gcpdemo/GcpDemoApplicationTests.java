package com.example.gcpdemo;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubOperations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
class GcpDemoApplicationTests {

  public static PubSubEmulatorContainer pubsub = new PubSubEmulatorContainer(
      DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:316.0.0-emulators")
  );

  @Autowired
  PubSubOperations pubSubOperations;

  @Autowired
  GcpDemoApplication.MyService myService;

  @DynamicPropertySource
  static void emulatorProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.cloud.gcp.pubsub.emulator-host", pubsub::getEmulatorEndpoint);
  }

  @BeforeAll
  static void setup() throws Exception {
    pubsub.start();
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("dns:///" + pubsub.getEmulatorEndpoint())
            .usePlaintext()
            .build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

    TopicAdminClient topicAdminClient =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(channelProvider)
                .build());

    SubscriptionAdminClient subscriptionAdminClient =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build());

    PubSubAdmin admin =
        new PubSubAdmin(() -> "kazuki43zoo-gcp-test", topicAdminClient, subscriptionAdminClient);

    admin.createTopic("someTopic");
    admin.createSubscription("someTopic-sub", "someTopic");

    admin.close();
    channel.shutdown();
  }

  @AfterAll
  static void tearDown() {
    pubsub.stop();
  }

  @Test
  void contextLoads() throws Exception {
    pubSubOperations.publish("someTopic", "Hello World!");
    Assertions.assertThat(myService.pollMessage().getPubsubMessage().getData().toStringUtf8()).isEqualTo("Hello World!");
  }

}
