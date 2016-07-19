package com.bgch.edge.scaling.platform;

import honeycomb.messages.MessageProtos;

public interface PlatformConsumer {
    void handleConnect(MessageProtos.Connect connect);
    void handleDisconnect(MessageProtos.Disconnect disconnect);
    void handleReport(MessageProtos.Report report);
}
