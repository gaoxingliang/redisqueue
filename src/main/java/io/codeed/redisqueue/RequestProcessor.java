package io.codeed.redisqueue;

public interface RequestProcessor {
    String process(String requestId, String data) throws Exception;

    void handleError(String requestId, String data, Exception e);
}
