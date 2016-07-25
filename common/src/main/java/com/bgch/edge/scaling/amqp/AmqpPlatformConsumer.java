package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.platform.ConnectHandler;
import com.bgch.edge.scaling.platform.DisconnectHandler;
import com.bgch.edge.scaling.platform.PlatformConsumer;
import com.bgch.edge.scaling.platform.ReportHandler;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.*;
import honeycomb.messages.MessageProtos;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class AmqpPlatformConsumer implements PlatformConsumer {
    private final Channel channel;
    private final List<ConnectHandler> connectHandlers = Lists.newArrayList();
    private final List<DisconnectHandler> disconnectHandlers = Lists.newArrayList();
    private final List<ReportHandler> reportHandlers = Lists.newArrayList();

    public AmqpPlatformConsumer(final Connection connection) throws IOException {
        this.channel = connection.createChannel();

        channel.exchangeDeclare(Exchanges.FROM_DEVICE, "direct", true);
        declareBindAndConsume(Queues.CONNECTED, connectConsumer(channel));
        declareBindAndConsume(Queues.DISCONNECTED, disconnectConsumer(channel));
        declareBindAndConsume(Queues.REPORT, reportConsumer(channel));
    }

    private DefaultConsumer connectConsumer(final Channel channel)  {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    final MessageProtos.Connect connect = MessageProtos.Connect.parseFrom(body);
                    connectHandlers.forEach(h -> h.handle(connect));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
    }

    private DefaultConsumer disconnectConsumer(final Channel channel)  {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    final MessageProtos.Disconnect disconnect = MessageProtos.Disconnect.parseFrom(body);
                    disconnectHandlers.forEach(h -> h.handle(disconnect));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
    }

    private DefaultConsumer reportConsumer(final Channel channel)  {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    final MessageProtos.Report report = MessageProtos.Report.parseFrom(body);
                    reportHandlers.forEach(h -> h.handle(report));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
    }

    private void declareBindAndConsume(final String queue, final DefaultConsumer consumer) throws IOException {
        channel.queueDeclare(queue, true, false, false, Collections.emptyMap());
        channel.queueBind(queue, Exchanges.FROM_DEVICE, queue);

        channel.basicConsume(queue, false, consumer);
    }

    @Override
    public void registerHandler(final ConnectHandler handler) {
        connectHandlers.add(handler);
    }

    @Override
    public void registerHandler(final DisconnectHandler handler) {
        disconnectHandlers.add(handler);
    }

    @Override
    public void registerHandler(final ReportHandler handler) {
        reportHandlers.add(handler);
    }
}
