package ru.arapov.ordermicroservice;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public String createOrder(@RequestBody OrderRequest request) {

        Map<String, Integer> items = new HashMap<>();
        items.put("popa", 2);
        items.put("pisya", 3);

        orderService.createOrder(
                request.getCustomerId(),
                request.getAmount(),
                items
        );

        return "Order creation init!";
    }
}
