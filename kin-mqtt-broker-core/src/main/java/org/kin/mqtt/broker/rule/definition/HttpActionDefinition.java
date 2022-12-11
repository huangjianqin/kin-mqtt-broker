package org.kin.mqtt.broker.rule.definition;

import org.kin.mqtt.broker.rule.impl.HttpBridgeAction;

import java.util.Collections;
import java.util.Map;

/**
 * http bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see HttpBridgeAction
 */
public class HttpActionDefinition extends BridgeActionDefinition {
    private String uri;
    private Map<String, Object> headers = Collections.emptyMap();

    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<HttpActionDefinition> {
        protected Builder() {
            super(new HttpActionDefinition());
        }

        public Builder uri(String uri) {
            definition.uri = uri;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            definition.headers = headers;
            return this;
        }
    }

    //setter && getter
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    @Override
    public String toString() {
        return "HttpActionDefinition{" +
                super.toString() +
                "uri='" + uri + '\'' +
                ", headers=" + headers +
                "} ";
    }
}
