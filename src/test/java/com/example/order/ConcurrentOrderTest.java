package com.example.order;

import com.example.order.common.IdempotentService;
import com.example.order.inventory.entity.Inventory;
import com.example.order.inventory.service.InventoryService;
import com.example.order.order.entity.CreateOrderRequest;
import com.example.order.order.service.OrderService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
public class ConcurrentOrderTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private IdempotentService idempotentService;

    /**
     * Test idempotent: same token, 20 concurrent requests, only 1 should succeed.
     */
    @Test
    void testIdempotent() throws InterruptedException {
        // Generate ONE token
        String token = idempotentService.generateToken();
        System.out.println("Token: " + token);

        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    ready.countDown();
                    start.await();
                    CreateOrderRequest request = new CreateOrderRequest();
                    request.setUserId(1L);
                    request.setProductId(1L);
                    request.setQuantity(1);
                    request.setIdempotentToken(token); // ALL threads use SAME token
                    orderService.createOrder(request);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        done.await();
        executor.shutdown();

        System.out.println("========== IDEMPOTENT TEST RESULT ==========");
        System.out.println("Token:            " + token);
        System.out.println("Total threads:    " + threadCount);
        System.out.println("Success orders:   " + successCount.get());
        System.out.println("Failed orders:    " + failCount.get());

        if (successCount.get() == 1) {
            System.out.println(">>> IDEMPOTENT OK! Only 1 order created.");
        } else {
            System.out.println(">>> PROBLEM: " + successCount.get() + " orders created!");
        }
        System.out.println("=============================================");
    }

    /**
     * Oversell test (keep for regression).
     * Before running: UPDATE inventory SET available_stock=5, locked_stock=0, total_stock=5 WHERE product_id=5;
     *                 DELETE FROM orders WHERE product_id=5;
     */
    @Test
    void testOversell() throws InterruptedException {
        Long productId = 5L;
        int threadCount = 50;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final long userId = 100 + i;
            executor.submit(() -> {
                try {
                    ready.countDown();
                    start.await();
                    // Each thread gets its OWN token (different users)
                    String token = idempotentService.generateToken();
                    CreateOrderRequest request = new CreateOrderRequest();
                    request.setUserId(userId);
                    request.setProductId(productId);
                    request.setQuantity(1);
                    request.setIdempotentToken(token);
                    orderService.createOrder(request);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        start.countDown();
        done.await();
        executor.shutdown();

        Inventory inventory = inventoryService.getByProductId(productId);

        System.out.println("========== OVERSELL TEST RESULT ==========");
        System.out.println("Total threads:    " + threadCount);
        System.out.println("Success orders:   " + successCount.get());
        System.out.println("Failed orders:    " + failCount.get());
        System.out.println("Available stock:  " + inventory.getAvailableStock());
        System.out.println("Locked stock:     " + inventory.getLockedStock());
        System.out.println("Total stock:      " + inventory.getTotalStock());

        if (successCount.get() > 5) {
            System.out.println(">>> OVERSELL DETECTED!");
        } else {
            System.out.println(">>> No oversell.");
        }
        System.out.println("==========================================");
    }
}