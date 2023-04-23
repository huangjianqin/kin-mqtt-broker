package org.kin.mqtt.broker.example;

/**
 * @author huangjianqin
 * @date 2023/4/22
 */
public interface Brokers {
    String B1 = "tcp://127.0.0.1:1883";
    String B2 = "tcp://127.0.0.1:1884";
    String B3 = "tcp://127.0.0.1:1885";
    String[] ALL = new String[]{B1, B2, B3};
}
