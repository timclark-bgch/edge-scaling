package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.device.DevicePublisher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import honeycomb.messages.MessageProtos;

import java.io.IOException;
import java.util.Collections;

public final class AmqpDevicePublisher implements DevicePublisher {
    private final Channel channel;

    public AmqpDevicePublisher(final Connection connection) throws IOException {
        this.channel = connection.createChannel();

        channel.exchangeDeclare(Exchanges.DIRECT, "direct", true);
        channel.queueDeclare(Queues.CONNECTED, true, false, false, Collections.emptyMap());
        channel.queueDeclare(Queues.DISCONNECTED, true, false, false, Collections.emptyMap());
        channel.queueDeclare(Queues.REPORT, true, false, false, Collections.emptyMap());
    }

    @Override
    public boolean connect(final MessageProtos.Connect connect) {
        return publish(Queues.CONNECTED, connect.toByteArray());
    }

    @Override
    public boolean disconnect(final MessageProtos.Disconnect disconnect) {
        return publish(Queues.DISCONNECTED, disconnect.toByteArray());
    }

    @Override
    public boolean report(final MessageProtos.Report report) {
        return publish(Queues.REPORT, report.toByteArray());
    }

    private boolean publish(final String routingKey, final byte[] payload) {
        try {
            synchronized (channel) {
                channel.basicPublish(Exchanges.DIRECT, routingKey, null, payload);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
