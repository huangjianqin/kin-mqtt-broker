package org.kin.mqtt.example.web;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @author huangjianqin
 * @date 2022/12/24
 */
@RestController
@RequestMapping("/mqtt")
public class MqttMessageController {
    @PostMapping("/receive")
    public void receive(@RequestBody Map<String, Object> jsonDataMap) {
        System.out.println(jsonDataMap);
    }
}
