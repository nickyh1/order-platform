package com.example.order.order.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.order.common.IdempotentService;
import com.example.order.common.Result;
import com.example.order.order.entity.CreateOrderRequest;
import com.example.order.order.entity.OrderInfo;
import com.example.order.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final IdempotentService idempotentService;

    /**
     * Get idempotent token before placing order
     */
    @GetMapping("/token")
    public Result<String> getToken() {
        return Result.success(idempotentService.generateToken());
    }

    /**
     * Create order
     */
    @PostMapping
    public Result<OrderInfo> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        return Result.success(orderService.createOrder(request));
    }

    /**
     * My orders (paginated)
     */
    @GetMapping("/user/{userId}")
    public Result<Page<OrderInfo>> listUserOrders(
            @PathVariable Long userId,
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "10") int pageSize) {
        return Result.success(orderService.listUserOrders(userId, pageNum, pageSize));
    }

    /**
     * Order detail
     */
    @GetMapping("/{orderNo}")
    public Result<OrderInfo> detail(@PathVariable String orderNo) {
        return Result.success(orderService.getByOrderNo(orderNo));
    }

    /**
     * Cancel order
     */
    @PostMapping("/{orderNo}/cancel")
    public Result<OrderInfo> cancel(@PathVariable String orderNo) {
        return Result.success(orderService.cancelOrder(orderNo));
    }
}