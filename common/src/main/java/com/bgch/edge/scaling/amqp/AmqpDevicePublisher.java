package com.bgch.edge.scaling.amqp;

import com.bgch.edge.scaling.device.DevicePublisher;
import honeycomb.messages.MessageProtos;

public final class AmqpDevicePublisher implements DevicePublisher {
    @Override
    public boolean connect(final MessageProtos.Connect connect) {
        return false;
    }

    @Override
    public boolean disconnect(final MessageProtos.Disconnect disconnect) {
        return false;
    }

    @Override
    public boolean report(final MessageProtos.Report report) {
        return false;
    }
}
