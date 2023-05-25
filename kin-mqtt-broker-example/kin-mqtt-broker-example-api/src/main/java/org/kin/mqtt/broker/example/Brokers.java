package org.kin.mqtt.broker.example;

/**
 * @author huangjianqin
 * @date 2023/4/22
 */
public interface Brokers {
    String B1 = "tcp://localhost:1883";
    String B2 = "tcp://localhost:1884";
    String B3 = "tcp://localhost:1885";
    String B4 = "tcp://localhost:1886";
    String[] ALL = new String[]{B1, B2, B3};
}
