package com.bgch.edge.scaling.amqp;

import com.rabbitmq.client.*;
import honeycomb.messages.MessageProtos;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class AmqpDevicePublisherTest {

    @Ignore
    @Test
    public void shouldPublishMessages() throws IOException, InterruptedException {
        final Connection connection = connection();

        final AtomicInteger connected = new AtomicInteger(0);
        final AtomicInteger disconnected = new AtomicInteger(0);
        final AtomicInteger report = new AtomicInteger(0);
        if (connection != null) {
            final Channel channel = connection.createChannel();
            channel.exchangeDeclare(Exchanges.FROM_DEVICE, "direct", true);
            channel.queueDeclare(Queues.CONNECTED, true, false, false, Collections.emptyMap());
            channel.queueDeclare(Queues.DISCONNECTED, true, false, false, Collections.emptyMap());
            channel.queueDeclare(Queues.REPORT, true, false, false, Collections.emptyMap());
            channel.queueBind(Queues.CONNECTED, Exchanges.FROM_DEVICE, Queues.CONNECTED);
            channel.queueBind(Queues.DISCONNECTED, Exchanges.FROM_DEVICE, Queues.DISCONNECTED);
            channel.queueBind(Queues.REPORT, Exchanges.FROM_DEVICE, Queues.REPORT);

            addConsumer(channel, Queues.CONNECTED, connected);
            addConsumer(channel, Queues.DISCONNECTED, disconnected);
            addConsumer(channel, Queues.REPORT, report);

            final AmqpDevicePublisher publisher = new AmqpDevicePublisher(connection);

            publisher.connect(MessageProtos.Connect.newBuilder().setDevice("test").build());
            publisher.disconnect(MessageProtos.Disconnect.newBuilder().setDevice("test").build());
            publisher.report(MessageProtos.Report.newBuilder().setDevice("test").build());

            TimeUnit.SECONDS.sleep(2);
        }


        assertThat(connected.get()).isEqualTo(1);
        assertThat(disconnected.get()).isEqualTo(1);
        assertThat(report.get()).isEqualTo(1);
    }

    private void addConsumer(final Channel channel, final String queue, final AtomicInteger counter) throws IOException {
        channel.basicConsume(queue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                counter.getAndIncrement();
                channel.basicAck(envelope.getDeliveryTag(), false);
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