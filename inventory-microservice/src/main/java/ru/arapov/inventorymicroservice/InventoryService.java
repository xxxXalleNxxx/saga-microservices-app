package ru.arapov.inventorymicroservice;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class InventoryService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ConcurrentHashMap<String, InventoryItem> inventory = new ConcurrentHashMap<>();

    public InventoryService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        initializeInventory();
    }

    private void initializeInventory() {
        inventory.put("popa", InventoryItem.builder()
                .id("popa")
                .name("popa")
                .quantity(10)
                .reserved(0)
                .build());

        inventory.put("pisya", InventoryItem.builder()
                .id("pisya")
                .name("pisya")
                .quantity(50)
                .reserved(0)
                .build());

        log.info("Inventory initialized");
    }

    @KafkaListener(topics = "order-created", groupId = "inventory-service")
    public void handleOrderCreated(String jsonEvent) {
        log.info("Received order event: {}", jsonEvent);

        try {
            OrderCreatedEvent event = objectMapper.readValue(jsonEvent, OrderCreatedEvent.class);

            String orderId = event.getOrderId();
            String sagaId = event.getSagaId();

            log.info("rocessing inventory for order: {}, Saga: {}", orderId, sagaId);

            Thread.sleep(300);

            boolean success = true;
            String message = "All items reserved successfully";
            Map<String, Integer> reservedItems = new HashMap<>();

            for (Map.Entry<String, Integer> entry : event.getItems().entrySet()) {
                String itemId = entry.getKey();
                int requestedQuantity = entry.getValue();

                InventoryItem item = inventory.get(itemId);
                if (item == null) {
                    success = false;
                    message = "Item not found: " + itemId;
                    break;
                }

                if (item.getQuantity() - item.getReserved() < requestedQuantity) {
                    success = false;
                    message = "Insufficient stock for item: " + itemId;
                    break;
                }

                item.setReserved(item.getReserved() + requestedQuantity);
                inventory.put(itemId, item);
                reservedItems.put(itemId, requestedQuantity);

                log.info("Reserved {} of {}", requestedQuantity, itemId);
            }

            if (!success) {
                log.warn("Reservation failed: {}", message);
                for (Map.Entry<String, Integer> entry : reservedItems.entrySet()) {
                    InventoryItem item = inventory.get(entry.getKey());
                    if (item != null) {
                        item.setReserved(item.getReserved() - entry.getValue());
                        inventory.put(entry.getKey(), item);
                        log.info("Released {} of {}", entry.getValue(), entry.getKey());
                    }
                }
            }

            InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
                    sagaId,
                    orderId,
                    reservedItems,
                    success,
                    message
            );

            String jsonResponse = objectMapper.writeValueAsString(inventoryEvent);

            kafkaTemplate.send("inventory-reserved", jsonResponse);
            log.info("Sent InventoryReservedEvent: {}", message);

        } catch (InterruptedException e) {
            log.error("Processing interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error processing order: {}", e.getMessage());

            try {
                Map<?, ?> rawEvent = objectMapper.readValue(jsonEvent, Map.class);
                String sagaId = (String) rawEvent.get("sagaId");
                String orderId = (String) rawEvent.get("orderId");

                InventoryReservedEvent errorEvent = new InventoryReservedEvent(
                        sagaId,
                        orderId,
                        new HashMap<>(),
                        false,
                        "Inventory processing error: " + e.getMessage()
                );

                String jsonError = objectMapper.writeValueAsString(errorEvent);
                kafkaTemplate.send("inventory-reserved", jsonError);

            } catch (Exception ex) {
                log.error("Failed to send error event", ex);
            }
        }
    }

    @KafkaListener(topics = "order-cancelled", groupId = "inventory-service")
    public void handleOrderCancelled(String jsonEvent) {
        try {
            OrderCancelledEvent event = objectMapper.readValue(jsonEvent, OrderCancelledEvent.class);

            log.info("Releasing inventory for cancelled order: {}", event.getOrderId());

            log.info("Inventory released for order: {}", event.getOrderId());

            InventoryReleasedEvent releaseEvent = new InventoryReleasedEvent(
                    event.getSagaId(),
                    event.getOrderId()
            );
            String jsonRelease = objectMapper.writeValueAsString(releaseEvent);

            kafkaTemplate.send("inventory-released", jsonRelease);
            log.info("Sent InventoryReleasedEvent");

        } catch (Exception e) {
            log.error("Failed to process cancellation", e);
        }
    }
}