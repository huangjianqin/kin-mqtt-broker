package org.kin.mqtt.broker.bridge;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;

/**
 * @author huangjianqin
 * @date 2022/11/22
 */
public abstract class NamedBridge implements Bridge {
    /** bridge name */
    private final String name;

    public NamedBridge(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "bridge name must not blank");
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}
