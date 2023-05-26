package org.kin.mqtt.broker.rule.action.bridge.definition;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;
import org.kin.mqtt.broker.core.Type;
import org.kin.mqtt.broker.rule.action.ActionType;
import org.kin.mqtt.broker.rule.action.bridge.HttpBridgeAction;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * http bridge动作规则定义
 *
 * @author huangjianqin
 * @date 2022/12/11
 * @see HttpBridgeAction
 */
@Type(ActionType.HTTP_BRIDGE)
public class HttpBridgeActionDefinition extends BridgeActionDefinition {
    private String uri;
    private Map<String, String> headers = Collections.emptyMap();

    private HttpBridgeActionDefinition() {
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void check() {
        super.check();
        Preconditions.checkArgument(StringUtils.isNotBlank(uri), "http uri must be not blank");
    }

    /** builder **/
    public static class Builder extends BridgeActionDefinition.Builder<HttpBridgeActionDefinition, Builder> {
        protected Builder() {
            super(new HttpBridgeActionDefinition());
        }

        public Builder uri(String uri) {
            definition.uri = uri;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
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

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        HttpBridgeActionDefinition that = (HttpBridgeActionDefinition) o;
        return Objects.equals(uri, that.uri) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), uri, headers);
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
