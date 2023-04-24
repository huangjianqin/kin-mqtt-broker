package org.kin.mqtt.broker.core;

import io.netty.util.HashedWheelTimer;
import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.CollectionUtils;
import org.kin.mqtt.broker.core.retry.PublishRetry;
import org.kin.mqtt.broker.core.retry.Retry;
import org.kin.mqtt.broker.core.retry.RetryService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/13
 */
final class DefaultRetryService extends HashedWheelTimer implements RetryService {
    /** key -> retry id, value -> {@link Retry} */
    private final Map<Long, Retry> id2Retry = new NonBlockingHashMap<>();

    DefaultRetryService() {
        //相当于50ms检查一次retry timeout
        super(50, TimeUnit.MILLISECONDS, 20);
    }

    @Override
    public void execRetry(Retry retry) {
        Retry curRetry = id2Retry.computeIfAbsent(retry.getId(), k -> retry);
        if (curRetry != retry) {
            throw new IllegalStateException(String.format("retry for id=%s has been executed", retry.getId()));
        }

        newTimeout(retry, retry.getDelay(), retry.getUnit());
    }

    @Override
    public Retry getRetry(long id) {
        return id2Retry.get(id);
    }

    @Override
    public void removeRetry(long id) {
        id2Retry.remove(id);
    }

    @Override
    public void close() {
        List<PublishRetry> retries = stop().stream().map(t -> (PublishRetry) t.task()).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(retries)) {
            return;
        }

        //取消所有没执行完的retry task, 再一次stop timer
        for (PublishRetry retry : retries) {
            retry.cancel();
        }
    }
}
