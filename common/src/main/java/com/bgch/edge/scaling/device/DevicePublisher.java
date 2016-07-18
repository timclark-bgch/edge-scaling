package com.bgch.edge.scaling.device;

import honeycomb.messages.MessageProtos;

public interface DevicePublisher {
    boolean connect(MessageProtos.Connect connect);
    boolean disconnect(MessageProtos.Disconnect disconnect);
    boolean report(MessageProtos.Report report);
}
