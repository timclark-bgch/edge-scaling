package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.platform.PlatformPublisher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import honeycomb.messages.MessageProtos;

import java.io.IOException;

public final class AmqpPlatformPublisher implements PlatformPublisher {
    private final Channel channel;

    public AmqpPlatformPublisher(final Connection connection) throws IOException {
        this.channel = connection.createChannel();

        channel.exchangeDeclare(Exchanges.FROM_PLATFORM, "direct", true);
    }

    @Override
    public void sendCommand(final MessageProtos.Command command) {
        synchronized (channel) {
            try {
                channel.basicPublish(Exchanges.FROM_PLATFORM, command.getDevice(), null, command.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
