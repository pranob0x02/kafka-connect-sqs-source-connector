package io.connect.sqs;

import io.connect.sqs.aws.SqsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deletes SQS messages on a background thread, in batches of up to 10, as soon as they are queued.
 */
public class MessageDeleter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageDeleter.class);

    private static final int MAX_BATCH_SIZE = 10;
    private static final long POLL_TIMEOUT_MS = 100L;
    private static final long CLOSE_TIMEOUT_MS = 5000L;

    private final SqsClient sqsClient;
    private final AtomicLong messagesDeleted;
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final Thread thread;
    private volatile boolean running = true;

    public MessageDeleter(SqsClient sqsClient, AtomicLong messagesDeleted) {
        this.sqsClient = sqsClient;
        this.messagesDeleted = messagesDeleted;

        // Carry the Connect task's logging context (connector|task) over to the deleter thread
        Map<String, String> loggingContext = MDC.getCopyOfContextMap();
        this.thread = new Thread(() -> {
            if (loggingContext != null) {
                MDC.setContextMap(loggingContext);
            }
            deleteQueuedMessages();
        }, "sqs-message-deleter");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /**
     * Queues a message for deletion. Never blocks, so it is safe to call from Kafka producer callbacks.
     */
    public void delete(Message message) {
        queue.add(message);
    }

    public int getQueuedCount() {
        return queue.size();
    }

    private void deleteQueuedMessages() {
        while (running || !queue.isEmpty()) {
            List<Message> batch = new ArrayList<>(MAX_BATCH_SIZE);
            try {
                Message first = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, MAX_BATCH_SIZE - 1);
                sqsClient.deleteMessages(batch);
                messagesDeleted.addAndGet(batch.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Failed to delete {} SQS messages; they will reappear after the visibility timeout",
                        batch.size(), e);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            thread.join(CLOSE_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            log.warn("Timed out deleting {} queued SQS messages; they will reappear after the visibility timeout",
                    queue.size());
            queue.clear();
            thread.interrupt();
        }
    }
}
