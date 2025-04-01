package io.codeed.redisqueue;

import lombok.extern.log4j.*;
import org.apache.commons.lang3.*;
import org.apache.commons.lang3.exception.*;
import org.springframework.data.redis.*;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.stream.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Redis queue implementation using redis stream
 */
@Log4j2
public class RedisQueue {
    private static final String KEY_STREAM_DATA = "_streamData";
    private static final String KEY_REQUEST_ID = "_requestId";
    private static final String KEY_ERROR = "_error";

    private final RedisTemplate<String, String> redisTemplate;
    private final String requestConsumerGroupName;
    private final String requestQueueName;
    private final String responseQueueName;
    private final String responseConsumerGroupName;
    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private final ConcurrentSkipListSet<String> ongoingRequest = new ConcurrentSkipListSet<>();
    private final Map<String, CompletableFuture<String>> responseMap = new ConcurrentHashMap<>();
    private boolean inited = false;

    public RedisQueue(RedisTemplate<String, String> redisTemplate, String queueName, StreamMessageListenerContainer<String,
            MapRecord<String, String, String>> container) {
        this.redisTemplate = redisTemplate;
        this.requestQueueName = "request-stream-" + queueName;
        this.responseQueueName = "response-stream-" + queueName;
        this.container = container;
        this.requestConsumerGroupName = "request-stream-cg-" + queueName;
        this.responseConsumerGroupName = "response-stream-cg-" + queueName;
    }

    public void init(boolean supportEnqueue, boolean supportDequeue, RequestProcessor requestProcessor) {
        if (inited) {
            return;
        }
        if (supportEnqueue) {
            initResponseListener();
        }
        if (supportDequeue) {
            initRequestProcessListener(requestProcessor);
        }
        synchronized (container) {
            if (!container.isRunning()) {
                container.start();
            }
        }
        inited = true;
    }

    private Map<String, String> mapOfData(String requestId, String data, Exception e) {
        Map<String, String> map = new HashMap<>();
        map.put(KEY_STREAM_DATA, data);
        map.put(KEY_REQUEST_ID, requestId);
        map.put(KEY_ERROR, e == null ? "" : StringUtils.firstNonEmpty(ExceptionUtils.getRootCauseMessage(e), "内部错误"));

        return map;
    }


    private void initRequestProcessListener(RequestProcessor requestProcessor) {
        createGroupQuietly(requestQueueName, requestConsumerGroupName);

        String consumerName = "consumer-" + RandomStringUtils.randomAlphabetic(5);
        container.receive(Consumer.from(requestConsumerGroupName, consumerName), StreamOffset.create(requestQueueName,
                ReadOffset.lastConsumed()), message -> {
            String requestId = message.getValue().get(KEY_REQUEST_ID);
            String data = message.getValue().get(KEY_STREAM_DATA);
            String result = null;
            try {
                result = requestProcessor.process(requestId, data);
                redisTemplate.opsForStream().add(responseQueueName, mapOfData(requestId, result, null));
            } catch (Exception e) {
                requestProcessor.handleError(requestId, data, e);
                redisTemplate.opsForStream().add(responseQueueName, mapOfData(requestId, result, e));
            }

            // 确认消息已处理
            redisTemplate.opsForStream().acknowledge(responseQueueName, requestConsumerGroupName, message.getId());
        });
    }

    /**
     * enqueue a data and don't need the response
     *
     * @param data
     * @throws Exception
     */
    public void enqueue(String data) throws Exception {
        enqueueAndWaitResponse(data, -1);
    }

    /**
     * 入队
     *
     * @param data
     * @param timeoutInSeconds 超时时间，单位秒，小于等于0表示不等待响应
     * @return 不等待响应时返回null
     * @throws Exception
     */
    public String enqueueAndWaitResponse(String data, int timeoutInSeconds) throws Exception {
        if (!inited) {
            throw new IllegalStateException("请先调用init方法");
        }
        String requestId = UUID.randomUUID().toString();
        CompletableFuture<String> future = new CompletableFuture<>();
        responseMap.put(requestId, future);
        ongoingRequest.add(requestId);

        RecordId recordId = redisTemplate.opsForStream().add(requestQueueName, mapOfData(requestId, data, null));
        log.info("加入队列：{}， {}", recordId.getValue(), StringUtils.abbreviate(data, 256));
        if (timeoutInSeconds > 0) {
            try {
                return future.get(timeoutInSeconds, TimeUnit.SECONDS); // 设置30秒超时
            } catch (TimeoutException e) {
                log.error("等待响应错误：{}, {}", requestId, recordId.getValue(), e);
                throw e;
            } catch (ExecutionException e) {
                log.error("等待响应执行错误：{}, {}", requestId, recordId.getValue(), e);
                throw e;
            }
            finally {
                responseMap.remove(requestId);
                ongoingRequest.remove(requestId);
            }
        } else {
            return null;
        }
    }

    private void createGroupQuietly(String queueName, String groupName) {
        try {
            redisTemplate.opsForStream().createGroup(queueName, groupName);
        } catch (Exception e) {
            String rootMsg = ExceptionUtils.getRootCauseMessage(e);
            if (rootMsg.contains("BUSYGROUP")) {
                log.info("Consumer group {} already exists for {}", responseConsumerGroupName, responseQueueName);
            } else {
                throw e;
            }
        }
    }

    private void initResponseListener() {
        createGroupQuietly(responseQueueName, responseConsumerGroupName);
        container.receive(Consumer.from(responseConsumerGroupName, RandomStringUtils.randomAlphabetic(5)),
                StreamOffset.create(responseQueueName, ReadOffset.lastConsumed()), message -> {
            String requestId = message.getValue().get(KEY_REQUEST_ID);
            String response = message.getValue().get(KEY_STREAM_DATA);

            CompletableFuture<String> future = responseMap.get(requestId);
            if (future != null) {
                future.complete(response);
            }

            // 确认消息已处理
            redisTemplate.opsForStream().acknowledge(responseQueueName, responseConsumerGroupName, message.getId());
        });
    }
}


