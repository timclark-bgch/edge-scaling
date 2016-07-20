package com.bgch.edge.scaling.platform;

public interface PlatformConsumer {
    void registerHandler(ConnectHandler handler);
    void registerHandler(DisconnectHandler handler);
    void registerHandler(ReportHandler handler);
}
