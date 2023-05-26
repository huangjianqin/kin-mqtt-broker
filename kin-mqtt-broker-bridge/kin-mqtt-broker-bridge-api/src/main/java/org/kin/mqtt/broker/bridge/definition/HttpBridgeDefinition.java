package org.kin.mqtt.broker.bridge.definition;

import org.kin.mqtt.broker.bridge.BridgeType;
import org.kin.mqtt.broker.core.Type;

import java.util.Collections;
import java.util.Map;

/**
 * http bridge配置定义
 * @author huangjianqin
 * @date 2023/5/26
 */
@Type(BridgeType.HTTP)
public class HttpBridgeDefinition extends AbstractBridgeDefinition{
    /** 默认headers */
    private Map<String, String> headers = Collections.emptyMap();

    public static Builder builder() {
        return new Builder();
    }


    /** builder **/
    public static class Builder extends AbstractBridgeDefinition.Builder<HttpBridgeDefinition, Builder> {
        protected Builder() {
            super(new HttpBridgeDefinition());
        }

        public Builder header(String key, String value) {
            definition.headers.put(key, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            definition.headers.putAll(headers);
            return this;
        }
    }

    //setter && getter
    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public String toString() {
        return "HttpBridgeDefinition{" +
                super.toString() +
                ", headers=" + headers +
                '}';
    }
}
