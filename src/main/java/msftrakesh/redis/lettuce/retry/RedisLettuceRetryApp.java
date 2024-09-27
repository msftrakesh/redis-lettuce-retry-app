/*

Disclaimer:

This open-source software is provided "as-is," without any express or implied warranties or guarantees. Neither the author(s) nor Microsoft Corporation make any representations or warranties, express or implied, regarding the accuracy, completeness, or performance of the software.

By using this software, you acknowledge and agree that the author(s) and Microsoft Corporation are not liable for any damages, losses, or issues that may arise from its use, including but not limited to direct, indirect, incidental, consequential, or punitive damages, regardless of the legal theory under which such claims arise.

This software is made available for free and open use, and it is your responsibility to test, evaluate, and use it at your own risk. The author(s) and Microsoft Corporation have no obligation to provide maintenance, support, updates, enhancements, or modifications to the software.

*/

package msftrakesh.redis.lettuce.retry;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SocketOptions.KeepAliveOptions;
import io.lettuce.core.SocketOptions.TcpUserTimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.event.Event;
import io.lettuce.core.event.EventBus;
import io.lettuce.core.event.connection.ConnectedEvent;
import io.lettuce.core.event.connection.ConnectionActivatedEvent;
import io.lettuce.core.event.connection.ConnectionDeactivatedEvent;
import io.lettuce.core.event.connection.ConnectionEvent;
import io.lettuce.core.event.connection.DisconnectedEvent;
import io.lettuce.core.event.connection.ReconnectAttemptEvent;
import io.lettuce.core.event.metrics.CommandLatencyEvent;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;

/**
 *
 * @author rakeshangi
 */
public class RedisLettuceRetryApp {
 
    private static final int TCP_KEEPALIVE_IDLE = 30;
    private static final int TCP_USER_TIMEOUT = 30;

    private static  Duration reconnectMinInterval = Duration.ofSeconds(30); // Min 30 seconds between reconnects
    private static  Duration reconnectMaxDuration = Duration.ofMinutes(30); // Give up after 30 minutes
    private static final Semaphore reconnectSemaphore = new Semaphore(1);
    private static long firstFailureTime = -1;
    private static long lastReconnectTime = System.currentTimeMillis();

     private static RedisClient redisClient = null;
     private static StatefulRedisConnection<String, byte[]> connection = null;
     private static RedisAsyncCommands<String, byte[]> asyncCommands = null;

    private static AppSettings appSettings = null;

    static class StringByteArrayCodec implements RedisCodec<String, byte[]> {

        @Override
        public String decodeKey(ByteBuffer bytes) {
            return StandardCharsets.UTF_8.decode(bytes).toString();
        }

        @Override
        public byte[] decodeValue(ByteBuffer bytes) {
            byte[] array = new byte[bytes.remaining()];
            bytes.get(array);
            return array;
        }

        @Override
        public ByteBuffer encodeKey(String key) {
            return StandardCharsets.UTF_8.encode(key);
        }

        @Override
        public ByteBuffer encodeValue(byte[] value) {
            return ByteBuffer.wrap(value);
        }
    }

    

    
    public static void main(String[] args) {

        log("Starting the test....");

        appSettings = new AppSettings("src/main/resources/config.yaml");
        
        reconnectMinInterval = Duration.ofSeconds(appSettings.getReconnectMinInterval()); 
        reconnectMaxDuration = Duration.ofMinutes(appSettings.getReconnectMaxDuration()); 

       
        Set<String> keys = new HashSet<>();
        Set<String> writtenKeys = new HashSet<>();

        try {

            redisClient = createRedisClient();
            connection = redisClient.connect(new StringByteArrayCodec());
            asyncCommands = connection.async();

            byte[] imageBytes = readImageFile("1mb_sample.jpg");

            Random random = new Random();
            Set<CompletableFuture<Void>> futures = new HashSet<>();

            // Write 10 key-value pairs asynchronously 
            IntStream.range(0, 10).forEach(i -> {
                String key = "imageKey_" + i;
                keys.add(key);

                CompletableFuture<Void> future = writeWithReconnect(key, imageBytes)
                        .thenCompose(result -> {
                            log("Write " + i + ": Completed for key " + key);
                            synchronized (writtenKeys) {
                                writtenKeys.add(key);
                            }
                            return CompletableFuture.completedFuture(null);
                        });

                futures.add(future);
            });

            // Wait for all writes to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            futures.clear();

            Set<CompletableFuture<byte[]>> read_futures = new HashSet<>();

            // Perform  random asynchronous reads
            IntStream.range(0, 100).forEach(i -> {
                CompletableFuture<byte[]> future;
                synchronized (writtenKeys) {
                    if (!writtenKeys.isEmpty()) {
                        String randomWrittenKey = writtenKeys.stream().skip(random.nextInt(writtenKeys.size())).findFirst().get();

                        future = readWithReconnect(randomWrittenKey);
                        future.thenAccept(value -> {
                            if (value != null) {
                                log("Read " + i + ": Retrieved image data of length: " + value.length + " for key " + randomWrittenKey);
                            } else {
                                log("Read " + i + ": No data found for key " + randomWrittenKey);
                            }
                        }).thenApply(ignored -> null);

                        try {
                            Thread.sleep(2000); //2 sec               delay after every read 

                        } catch (InterruptedException e) {
                            logError("Thread was interrupted.");
                            Thread.currentThread().interrupt(); // Restore the interrupted status
                        }
                    } else {
                        log("Skipping read " + i + " as no keys have been written yet.");
                        future = CompletableFuture.completedFuture(null);
                    }
                }

                read_futures.add(future);
            });

            // Wait for all reads to complete
            CompletableFuture.allOf(read_futures.toArray(new CompletableFuture[0])).join();

        } finally {
            cleanupKeys(connection, keys);
            if (redisClient != null) {
                redisClient.shutdown();
            }
        }

        log("All operations completed.");

    }

    private static void log(String message) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println(timestamp + " - " + message);
    }

    private static void logError(String message) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.err.println(timestamp + " - " + message);
    }

    private static RedisClient createRedisClient() {

        RedisURI redisUri = createRedisURI();

        ClientResources clientResources = createClientResources();

        SocketOptions socketOptions = SocketOptions.builder()
                .keepAlive(KeepAliveOptions.builder()
                        .enable()
                        .idle(Duration.ofSeconds(TCP_KEEPALIVE_IDLE))
                        .interval(Duration.ofSeconds(TCP_KEEPALIVE_IDLE / 3))
                        .count(3)
                        .build())
                .tcpUserTimeout(TcpUserTimeoutOptions.builder()
                        .enable()
                        .tcpUserTimeout(Duration.ofSeconds(TCP_USER_TIMEOUT))
                        .build())
                .build();
        // Configure Client Options
        ClientOptions clientOptions = ClientOptions.builder()
                .autoReconnect(true)
                .cancelCommandsOnReconnectFailure(true)
             //   .socketOptions(socketOptions)
                .build();

        RedisClient redisClient1 = RedisClient.create(clientResources, redisUri);
        redisClient1.setOptions(clientOptions);
        redisClient1.setDefaultTimeout(Duration.ofSeconds(10));

        return redisClient1;
    }

    private static RedisURI createRedisURI() {
        String redisHostName = appSettings.getRedisHostName();
        int redisPort = appSettings.getRedisPort();
        String redisKey = appSettings.getRedisKey();

        return RedisURI.builder()
                .withHost(redisHostName)
                .withPort(redisPort)
                .withPassword(redisKey.toCharArray())
                .withSsl(true)
                .build();

    }

    private static ClientResources createClientResources() {

        // Custom Exponential Backoff Delay for reconnection
        //Full jitter introduces randomness into the delay to avoid the "thundering herd problem," where many clients might try to reconnect simultaneously, leading to potential overload on the target service. By adding jitter, each client waits a different amount of time before retrying, reducing the chance of simultaneous retries.
        Delay reconnectDelay = Delay.fullJitter(
                Duration.ofMillis(100), // minimum 100 millisecond delay
                Duration.ofSeconds(10), // maximum 10 second delay
                100, TimeUnit.MILLISECONDS);

        ClientResources clientResources = DefaultClientResources.builder()
                .reconnectDelay(reconnectDelay)
                .build();

        EventBus eventBus = clientResources.eventBus();

        addEventListener(eventBus);

        return clientResources;
    }

    // Generic event listener function
    private static void addEventListener(EventBus eventBus) {
        eventBus.get()
                .subscribe(event -> {

                    switch (event.getClass().getSimpleName()) {
                        case "ConnectionEvent" ->
                            handleConnectionEvent((ConnectionEvent) event);

                        case "ReconnectEvent" ->
                            handleReconnectEvent((ReconnectAttemptEvent) event);

                        case "CommandLatencyEvent" ->
                            handleCommandLatencyEvent((CommandLatencyEvent) event);

                        case "ConnectionActivatedEvent" ->
                            handleConnectionActivatedEvent((ConnectionActivatedEvent) event);

                        case "ConnectedEvent" ->
                            handleConnectedEvent((ConnectedEvent)event);

                        case "DisconnectedEvent" ->

                            handleDisconnectedEvent((DisconnectedEvent)event);

                        case "ConnectionDeactivatedEvent" ->
                            handleConnectionDeactivatedEvent((ConnectionDeactivatedEvent)event);

                        default ->
                            handleUnknownEvent(event);

                    }

                });
    }

    private static void handleConnectionActivatedEvent(ConnectionActivatedEvent event) {
        log("Connection Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleConnectedEvent(ConnectedEvent event) {
        log("Connection Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleDisconnectedEvent(DisconnectedEvent event) {
        log("Connection Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleConnectionDeactivatedEvent(ConnectionDeactivatedEvent event) {
        log("Connection Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }


    private static void handleConnectionEvent(ConnectionEvent event) {
        log("Connection Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleReconnectEvent(ReconnectAttemptEvent event) {
        log("Reconnect Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleCommandLatencyEvent(CommandLatencyEvent event) {
        log("Command Latency Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static void handleUnknownEvent(Event event) {
        log("Unknown Event: " + event.getClass().getSimpleName() + " - " + event.toString());
    }

    private static byte[] readImageFile(String filePath) {
        try {
            return Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to read image file: " + filePath, e);
        }
    }

    private static <T> CompletableFuture<T> convertToCompletableFuture(RedisFuture<T> redisFuture) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        redisFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                completableFuture.completeExceptionally(throwable);
            } else {
                completableFuture.complete(result);
            }
        });
        return completableFuture;
    }

    private static void cleanupKeys(StatefulRedisConnection<String, byte[]> connection, Set<String> keys) {
        if (connection != null) {
            RedisAsyncCommands<String, byte[]> asyncCommands = connection.async();
            keys.forEach(key -> {
                CompletableFuture<Long> delFuture = convertToCompletableFuture(asyncCommands.del(key));
                delFuture.thenAccept(result -> {
                    log("Deleted key: " + key);
                }).exceptionally(e -> {
                    e.printStackTrace();
                    return null;
                }).join();
            });
            connection.close();
        }

    }

    private static CompletableFuture<String> writeDataAsync(String key, byte[] value) {
        return convertToCompletableFuture(asyncCommands.set(key, value));
    }

    private static CompletableFuture<byte[]> readDataAsync(String key) {

        return convertToCompletableFuture(asyncCommands.get(key));
    }

    private static CompletableFuture<String> writeWithReconnect(String key, byte[] value) {
        CompletableFuture<String> future = new CompletableFuture<>();

        // Perform the initial write
        writeDataAsync(key, value)
                .thenAccept(result -> {
                    firstFailureTime = -1; // Reset firstFailureTime after every successful operation 
                    log("Write successful for key: " + key);
                    future.complete(result); // Mark the future as completed successfully
                })
                .exceptionally(throwable -> {
                    logError("Write failed for key: " + key + ", attempting reconnect...");

                    if (handleFailure()) {
                        // Retry writing the data after reconnect
                        writeDataAsync(key, value)
                                .thenAccept(retryResult -> {
                                    log("Write successful after reconnect for key: " + key);
                                    future.complete(retryResult); // Complete the future successfully after retry
                                })
                                .exceptionally(retryThrowable -> {
                                    logError("Write failed again after reconnect for key: " + key);
                                    future.complete(null); // Mark as failed
                                    return null; // Return null for exceptionally, since it's allowed here
                                });
                    } else {
                        logError("Reconnect failed, giving up write.");
                        future.complete(null); // Complete with null as reconnect failed
                    }
                    return null; // Return null to satisfy the lambda for exceptionally
                });

        return future;
    }

    private static CompletableFuture<byte[]> readWithReconnect(String key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        // Perform the initial read
        readDataAsync(key)
                .thenAccept(result -> {
                    firstFailureTime = -1; // Reset firstFailureTime after every successful operation 
                    log("Read successful for key: " + key);
                    future.complete(result); // Mark the future as completed successfully
                })
                .exceptionally(throwable -> {
                    logError("Read failed for key: " + key + ", attempting reconnect...");

                    if (handleFailure()) {
                        // Retry reading the data after reconnect
                        readDataAsync(key)
                                .thenAccept(retryResult -> {
                                    log("Read successful after reconnect for key: " + key);
                                    future.complete(retryResult); // Complete the future successfully after retry
                                })
                                .exceptionally(retryThrowable -> {
                                    logError("Read failed again after reconnect for key: " + key);
                                    future.complete(null); // Mark as failed
                                    return null; // Return null for exceptionally 
                                });
                    } else {
                        logError("Reconnect failed, reading from database...");
                        // Reconnect failed, fallback to database read
                        readFromDatabaseAsync(key, future);
                    }
                    return null; 
                });

        return future;
    }


    public static void CloseConnnection() {
        try {
            if (asyncCommands != null && asyncCommands.getStatefulConnection().isOpen()) {
                asyncCommands.getStatefulConnection().close();
                log("Closed the previous connection.");
            } else {
                log("Connection already closed, skipping close.");
            }
        } catch (Exception closeException) {
            logError("Failed to close the connection during reconnect: " + closeException.getMessage());
        }
    }

    private static boolean handleFailure() {

        if (firstFailureTime == -1) {
            firstFailureTime = System.currentTimeMillis();
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - firstFailureTime >= reconnectMaxDuration.toMillis()) {
            logError("Giving up reconnect attempts after 30 minutes.");
            return false;
        }

        if (currentTime - lastReconnectTime >= reconnectMinInterval.toMillis()) {
            if (forceReconnect()) {
                firstFailureTime = -1; // Reset firstFailureTime after successful reconnect
                return true;
            }
        }
        return false;
    }

    private static boolean forceReconnect() {
        CompletableFuture<RedisAsyncCommands<String, byte[]>> reconnectFuture = new CompletableFuture<>();
        long currentTime = System.currentTimeMillis();

        if (currentTime - lastReconnectTime < reconnectMinInterval.toMillis()) {
            log("Reconnect skipped, too soon since the last reconnect.");
            return false;
        }

        boolean lockTaken = false;
        try {
            // Try acquiring the semaphore
            lockTaken = reconnectSemaphore.tryAcquire(1, TimeUnit.SECONDS); // Reduce timeout for quicker failure
            if (!lockTaken) {
                log("Another thread is already reconnecting.");
                return false;
            }

           // log("Reconnecting...");
            // Close the previous connection properly
            CloseConnnection();

           // Perform the actual reconnection           
            connection = redisClient.connect(new StringByteArrayCodec());
            asyncCommands = connection.async(); // Reassign the new connection
            lastReconnectTime = System.currentTimeMillis();
            log("Reconnected successfully.");
            reconnectFuture.complete(asyncCommands); // Return the new async commands after successful reconnect
        } catch (Exception e) {
            logError("Failed to reconnect: " + e.getMessage());
            reconnectFuture.complete(null); // Reconnection failed
        } finally {
            if (lockTaken) {
                reconnectSemaphore.release();
            }
        }

        return true;
    }

     // Placeholder for database read if Redis fails

     private static void readFromDatabaseAsync(String key, CompletableFuture<byte[]> future) {
    CompletableFuture.runAsync(() -> {
        log("Reading from database for key: " + key);
        try {
            // Add database read logic here
            byte[] dbResult = readFromDatabase(key); // Assume this reads from DB
            future.complete(dbResult); // Complete with the DB result
        } catch (Exception e) {
            logError("Failed to read from database for key: " + key);
            future.completeExceptionally(e); // Mark the future as failed
        }
    });
}


    private static byte[] readFromDatabase(String key) {
        log("Reading from database for key: " + key);
        return readImageFile("1mb_sample.jpg");
    }



  

   
    
}

