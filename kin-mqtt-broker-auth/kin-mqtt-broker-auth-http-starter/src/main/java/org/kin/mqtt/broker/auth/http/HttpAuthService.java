package org.kin.mqtt.broker.auth.http;

import org.kin.framework.utils.JSON;
import org.kin.mqtt.broker.auth.AuthService;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/11/20
 */
public class HttpAuthService implements AuthService {
    /** http post url */
    private final String url;

    public HttpAuthService(String url) {
        this.url = url;
    }

    @Override
    public Mono<Boolean> auth(String userName, String password) {
        Map<String, Object> body = new HashMap<>(3);
        body.put("userName", userName);
        body.put("password", password);
        return WebClient.create()
                .post()
                .uri(url)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(JSON.write(body))
                .retrieve()
                .bodyToMono(Boolean.class);
    }
}
