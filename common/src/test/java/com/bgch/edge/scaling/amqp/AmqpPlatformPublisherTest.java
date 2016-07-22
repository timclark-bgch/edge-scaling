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

public class AmqpPlatformPublisherTest {

    @Ignore
    @Test
    public void shouldPublishMessages() throws IOException, InterruptedException {
        final Connection connection = connection();

        final AtomicInteger commands = new AtomicInteger(0);
        if (connection != null) {
            final String device = "test";
            final Channel channel = connection.createChannel();

            channel.exchangeDeclare(Exchanges.FROM_PLATFORM, "direct", true);
            channel.queueDeclare(device, true, true, false, Collections.emptyMap());
            channel.queueBind(device, Exchanges.FROM_PLATFORM, device);

            channel.basicConsume(device, false, new DefaultConsumer(channel){
                @Override
                public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                    commands.getAndIncrement();
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            });

            final AmqpPlatformPublisher publisher = new AmqpPlatformPublisher(connection);
            final MessageProtos.Command command = MessageProtos.Command.newBuilder().setDevice(device).build();
            publisher.sendCommand(command);
            publisher.sendCommand(command);

            TimeUnit.SECONDS.sleep(2);
        }

        assertThat(commands.get()).isEqualTo(2);
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