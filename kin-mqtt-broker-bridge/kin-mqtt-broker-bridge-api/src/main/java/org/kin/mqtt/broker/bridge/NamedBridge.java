package org.kin.mqtt.broker.bridge;

/**
 * @author huangjianqin
 * @date 2022/11/22
 */
public abstract class NamedBridge implements Bridge {
    /** bridge name */
    private final String name;

    public NamedBridge(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}
