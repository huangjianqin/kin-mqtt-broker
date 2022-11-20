package org.kin.mqtt.broker.auth.http;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
@ConfigurationProperties("org.kin.mqtt.broker.auth")
public class HttpAuthProperties {
    /** http post url */
    private String url;

    //setter && getter
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
