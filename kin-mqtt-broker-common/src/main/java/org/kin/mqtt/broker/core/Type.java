package org.kin.mqtt.broker.core;

import java.lang.annotation.*;

/**
 * 声明指定class关联的type name, 目前应用于action和bridge的相关定义
 * @author huangjianqin
 * @date 2023/5/26
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Type {
    /** type name */
    String value();
}
