# Redis Queue
A simple Redis queue implementation using Redis Stream for asynchronous request-response patterns.

## Maven Central
The library is available on Maven Central. You can add it to your project using the following dependency configurations:

### Maven
```xml
<dependency>
    <groupId>io.gitee.codeed</groupId>
    <artifactId>redisqueue</artifactId>
    <version>0.0.3</version>
</dependency>
```

### Gradle
```groovy
implementation 'io.gitee.codeed:redisqueue:0.0.3'
```

## How to Use

### Initialize the Queue

```java
@Autowired
private RedisTemplate<String, String> redisTemplate;
@Autowired
private StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamContainer;

// Create a new queue instance
RedisQueue redisQueue = new RedisQueue(redisTemplate, "my-queue-name", streamContainer);

// Initialize the queue with your requirements
// For sender (enqueue only):
redisQueue.init(true, false, null);

// For receiver (dequeue and process):
redisQueue.init(false, true, new RequestProcessor() {
    @Override
    public String process(String requestId, String data) {
        // Process your message here
        return "Response for " + data;
    }

    @Override
    public void handleError(String requestId, String data, Exception e) {
        // Handle any processing errors here
        log.error("Error processing request: " + requestId, e);
    }
});

// For bidirectional communication (both send and receive):
redisQueue.init(true, true, requestProcessor);
```

### 3. Basic Usage

```java
// Send a message without waiting for response
redisQueue.enqueue("Hello, Redis Queue!");

// Send a message and wait for response with timeout
try {
    String response = redisQueue.enqueueAndWaitResponse("Hello, need response!", 30); // 30 seconds timeout
    System.out.println("Received response: " + response);
} catch (TimeoutException e) {
    System.out.println("Request timed out");
} catch (Exception e) {
    System.out.println("Error: " + e.getMessage());
}
```

## Configuration Options

The Redis Queue has the following default settings that can be customized:
```java
// Set custom TTL (default is 60 minutes)
redisQueue.setStreamTtlMinutes(120L); // Set to 2 hours

// Set custom max length (default is 10,000 messages)
redisQueue.setMaxLength(20000L);
```

Default settings:
- Maximum queue length: 10,000 messages
- TTL (Time To Live): 60 minutes

## Implementation Details
- Uses Redis Streams for reliable message delivery
- Supports both fire-and-forget and request-response patterns
- Automatic message acknowledgment
- Automatic stream cleanup and TTL management
- Built-in error handling and timeout management

## Requirements
- Java 8 or higher
- Spring Boot 2.x
- Spring Data Redis
- Redis 5.0 or higher

## Note
Make sure to properly configure your Spring application with Redis connection details and create the necessary beans for RedisTemplate and StreamMessageListenerContainer.