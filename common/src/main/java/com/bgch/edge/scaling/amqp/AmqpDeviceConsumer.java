package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.device.CommandHandler;
import com.bgch.edge.scaling.device.DeviceConsumer;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.*;
import honeycomb.messages.MessageProtos;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class AmqpDeviceConsumer implements DeviceConsumer {
    private final Channel channel;
    private final DefaultConsumer consumer;
    private final List<CommandHandler> handlers = Lists.newArrayList();

    public AmqpDeviceConsumer(final Connection connection, final List<String> devices) throws IOException {
        this.channel = connection.createChannel();

        channel.exchangeDeclare(Exchanges.FROM_PLATFORM, "direct", true);
        this.consumer = consumer(channel);
        devices.forEach(this::declareAndBind);
    }

    private void declareAndBind(final String device) {
        try {
            channel.queueDeclare(device, true, true, false, Collections.emptyMap());
            channel.queueBind(device, Exchanges.FROM_PLATFORM, device);

            channel.basicConsume(device, false, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DefaultConsumer consumer(final Channel channel) {
        return new DefaultConsumer(channel)    {

            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    final MessageProtos.Command command = MessageProtos.Command.parseFrom(body);
                    handlers.forEach(h -> h.handle(command));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
    }

    @Override
    public void registerHandler(final CommandHandler handler) {
        handlers.add(handler);
    }
}
