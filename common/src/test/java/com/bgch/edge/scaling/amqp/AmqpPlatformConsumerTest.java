package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.platform.ConnectHandler;
import com.bgch.edge.scaling.platform.DisconnectHandler;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class AmqpPlatformConsumerTest {

    @Ignore
    @Test
    public void consumesMessages() throws IOException {
        final Connection connection = connection();

        final AtomicInteger connections = new AtomicInteger(0);
        final AtomicInteger disconnections = new AtomicInteger(0);
        final AtomicInteger reports = new AtomicInteger(0);

        if (connection != null) {
            final Channel channel = connection.createChannel();
            channel.exchangeDeclare(Exchanges.FROM_DEVICE, "direct", true);

            final AmqpPlatformConsumer consumer = new AmqpPlatformConsumer(connection);

            final ConnectHandler connectHandler = connect -> connections.getAndIncrement();
            consumer.registerHandler(connectHandler);

            final DisconnectHandler disconnectHandler = disconnect -> disconnections.getAndIncrement();
            consumer.registerHandler(disconnectHandler);

            final DisconnectHandler reportHandler = report -> reports.getAndIncrement();
            consumer.registerHandler(reportHandler);

            channel.basicPublish(Exchanges.FROM_DEVICE, Queues.CONNECTED, null, MessageProtos.Connect.newBuilder().setDevice("test").build().toByteArray());
            channel.basicPublish(Exchanges.FROM_DEVICE, Queues.DISCONNECTED, null, MessageProtos.Disconnect.newBuilder().setDevice("test").build().toByteArray());
            channel.basicPublish(Exchanges.FROM_DEVICE, Queues.REPORT, null, MessageProtos.Report.newBuilder().setDevice("test").build().toByteArray());

        }

        assertThat(connections.get()).isEqualTo(1);
        assertThat(disconnections.get()).isEqualTo(1);
        assertThat(reports.get()).isEqualTo(1);
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