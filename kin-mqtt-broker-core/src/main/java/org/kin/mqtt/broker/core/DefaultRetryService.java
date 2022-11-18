package org.kin.mqtt.broker.core;

import io.netty.util.HashedWheelTimer;
import org.jctools.maps.NonBlockingHashMap;
import org.kin.framework.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author huangjianqin
 * @date 2022/11/13
 */
final class DefaultRetryService extends HashedWheelTimer implements RetryService {
    private static final Logger log = LoggerFactory.getLogger(DefaultRetryService.class);
    /** key -> retry id, value -> {@link Retry} */
    private final Map<Long, Retry> id2Retry = new NonBlockingHashMap<>();

    DefaultRetryService() {
        //相当于50ms处理一次retry
        super(50, TimeUnit.MILLISECONDS, 20);
    }

    @Override
    public void execRetry(Retry retry) {
        Retry curRetry = id2Retry.computeIfAbsent(retry.getId(), k -> retry);
        if (curRetry != retry) {
            throw new IllegalStateException("retry for id=%s has been executed");
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

        log.warn("send publish message retry task still have {}, {}", retries.size(), retries);
        //取消所有没执行完的retry task, 再一次stop timer
        for (PublishRetry retry : retries) {
            retry.cancel();
        }
    }
}
