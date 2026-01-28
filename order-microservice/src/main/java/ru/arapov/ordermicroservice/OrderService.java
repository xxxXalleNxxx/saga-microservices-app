package ru.arapov.ordermicroservice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Order> orders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> sagaOrderMapping = new ConcurrentHashMap<>();

    public Order createOrder(String customerId, double amount, Map<String, Integer> items) {
        String sagaId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();

        Order order = Order.builder()
                .id(orderId)
                .customerId(customerId)
                .amount(amount)
                .status("PENDING")
                .items(items != null ? new HashMap<>(items) : new HashMap<>())
                .build();

        orders.put(orderId, order);
        sagaOrderMapping.put(sagaId, orderId);

        log.info("Creating order: {}, Saga: {}", orderId, sagaId);

        try {
            OrderCreatedEvent event = new OrderCreatedEvent(
                    sagaId, orderId, customerId, amount, items
            );
            String jsonEvent = objectMapper.writeValueAsString(event);

            kafkaTemplate.send("order-created", jsonEvent);
            log.info("Sent OrderCreatedEvent: {}", jsonEvent);

        } catch (Exception e) {
            log.error("Failed to send event", e);
        }

        return order;
    }

    @KafkaListener(topics = "payment-processed", groupId = "order-service")
    public void handlePaymentProcessed(String jsonEvent) {
        log.info("Received payment event: {}", jsonEvent);

        try {
            PaymentProcessedEvent event = objectMapper.readValue(jsonEvent, PaymentProcessedEvent.class);

            if (!event.isSuccess()) {
                String orderId = sagaOrderMapping.get(event.getSagaId());
                if (orderId != null) {
                    Order order = orders.get(orderId);
                    if (order != null && !"CANCELLED".equals(order.getStatus())) {
                        log.error("Payment failed! Cancelling order: {}", orderId);

                        order.setStatus("CANCELLED");
                        orders.put(orderId, order);

                        OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                                event.getSagaId(), orderId, "Payment failed: " + event.getMessage()
                        );
                        String jsonCancel = objectMapper.writeValueAsString(cancelEvent);

                        kafkaTemplate.send("order-cancelled", jsonCancel);
                        log.info("Sent OrderCancelledEvent");
                    }
                }
            } else {
                log.info("Payment successful for order: {}", event.getOrderId());
            }

        } catch (Exception e) {
            log.error("Failed to process payment event", e);
        }
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "order-service")
    public void handleInventoryReserved(String jsonEvent) {
        log.info("Received inventory event: {}", jsonEvent);

        try {
            InventoryReservedEvent event = objectMapper.readValue(jsonEvent, InventoryReservedEvent.class);

            if (!event.isSuccess()) {
                String orderId = sagaOrderMapping.get(event.getSagaId());
                if (orderId != null) {
                    Order order = orders.get(orderId);
                    if (order != null && !"CANCELLED".equals(order.getStatus())) {
                        log.error("Inventory reservation failed! Cancelling order: {}", orderId);

                        order.setStatus("CANCELLED");
                        orders.put(orderId, order);

                        OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                                event.getSagaId(), orderId, "Inventory reservation failed: " + event.getMessage()
                        );
                        String jsonCancel = objectMapper.writeValueAsString(cancelEvent);

                        kafkaTemplate.send("order-cancelled", jsonCancel);
                        log.info("Sent OrderCancelledEvent");

                        PaymentRefundedEvent refundEvent = new PaymentRefundedEvent(
                                event.getSagaId(), orderId, "refund-" + orderId
                        );
                        String jsonRefund = objectMapper.writeValueAsString(refundEvent);

                        kafkaTemplate.send("payment-refund", jsonRefund);
                        log.info("Triggered refund for failed inventory");
                    }
                }
            } else {
                String orderId = sagaOrderMapping.get(event.getSagaId());
                if (orderId != null) {
                    Order order = orders.get(orderId);
                    if (order != null && "PENDING".equals(order.getStatus())) {
                        log.info("Order completed successfully: {}", orderId);
                        order.setStatus("COMPLETED");
                        orders.put(orderId, order);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Failed to process inventory event", e);
        }
    }

    @KafkaListener(topics = "payment-refunded", groupId = "order-service")
    public void handlePaymentRefunded(String jsonEvent) {
        try {
            PaymentRefundedEvent event = objectMapper.readValue(jsonEvent, PaymentRefundedEvent.class);
            log.info("Payment refunded for order: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("Failed to process refund event", e);
        }
    }

    public Order getOrder(String orderId) {
        return orders.get(orderId);
    }
}
