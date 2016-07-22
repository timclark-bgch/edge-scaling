package com.bgch.edge.scaling.amqp;

import com.google.common.collect.Lists;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import honeycomb.messages.MessageProtos;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class AmqpDeviceConsumerTest {

    @Ignore
    @Test
    public void consumesMessages() throws IOException, InterruptedException {
        final Connection connection = connection();
        final List<String> devices = Lists.newArrayList("d1", "d2", "d3");

        final AtomicInteger commandsReceived = new AtomicInteger(0);
        if (connection != null) {
            final Channel channel = connection.createChannel();
            channel.exchangeDeclare(Exchanges.FROM_PLATFORM, "direct", true);

            final AmqpDeviceConsumer consumer = new AmqpDeviceConsumer(connection, devices);
            consumer.registerHandler(command -> commandsReceived.getAndIncrement());

            publish(channel, devices);
        }

        TimeUnit.SECONDS.sleep(2);
        assertThat(commandsReceived.get()).isEqualTo(devices.size());
    }

    private void publish(final Channel channel, final List<String> devices) {
        final MessageProtos.Command command = MessageProtos.Command.newBuilder().setDevice("test").build();
        devices.forEach(d -> {
            try {
                channel.basicPublish(Exchanges.FROM_PLATFORM, d, null, command.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private Connection connection() {
        final ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri("amqp://localhost:5672");
            return factory.newConnection();
        } catch (IOException | TimeoutException | NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }
}