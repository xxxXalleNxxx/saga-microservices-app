package ru.arapov.paymentmicroservice;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Payment> payments = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> sagaPaymentMapping = new ConcurrentHashMap<>();

    @KafkaListener(topics = "order-created", groupId = "payment-service")
    public void handleOrderCreated(String jsonEvent) {
        log.info("Received order event: {}", jsonEvent);

        try {
            OrderCreatedEvent event = objectMapper.readValue(jsonEvent, OrderCreatedEvent.class);

            String sagaId = event.getSagaId();
            String orderId = event.getOrderId();

            log.info("Processing payment for order: {}, Saga: {}", orderId, sagaId);

            Thread.sleep(500);

            boolean success = Math.random() > 0.3;
            String message = success ? "Payment processed successfully" : "Insufficient funds";

            String paymentId = "pay-" + UUID.randomUUID().toString();
            Payment payment = Payment.builder()
                    .id(paymentId)
                    .orderId(orderId)
                    .amount(event.getAmount())
                    .status(success ? "COMPLETED" : "FAILED")
                    .build();

            payments.put(paymentId, payment);
            sagaPaymentMapping.put(sagaId, paymentId);

            PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                    sagaId,
                    orderId,
                    paymentId,
                    event.getAmount(),
                    success,
                    message
            );

            String jsonPayment = objectMapper.writeValueAsString(paymentEvent);

            kafkaTemplate.send("payment-processed", jsonPayment);
            log.info("Sent PaymentProcessedEvent: {}", message);

        } catch (InterruptedException e) {
            log.error("Payment processing interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error processing payment: {}", e.getMessage());

            try {
                Map<?, ?> rawEvent = objectMapper.readValue(jsonEvent, Map.class);
                String sagaId = (String) rawEvent.get("sagaId");
                String orderId = (String) rawEvent.get("orderId");

                PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                        sagaId,
                        orderId,
                        "error-" + UUID.randomUUID().toString(),
                        0.0,
                        false,
                        "Payment processing error: " + e.getMessage()
                );

                String jsonError = objectMapper.writeValueAsString(paymentEvent);
                kafkaTemplate.send("payment-processed", jsonError);

            } catch (Exception ex) {
                log.error("Failed to send error event", ex);
            }
        }
    }

    @KafkaListener(topics = "payment-refund", groupId = "payment-service")
    public void handlePaymentRefund(String jsonEvent) {
        log.info("Received refund request: {}", jsonEvent);

        try {
            PaymentRefundedEvent event = objectMapper.readValue(jsonEvent, PaymentRefundedEvent.class);

            String paymentId = sagaPaymentMapping.get(event.getSagaId());
            if (paymentId != null) {
                Payment payment = payments.get(paymentId);
                if (payment != null) {
                    log.info("Processing refund for payment: {}", paymentId);

                    payment.setStatus("REFUNDED");
                    payments.put(paymentId, payment);

                    PaymentRefundedEvent refundEvent = new PaymentRefundedEvent(
                            event.getSagaId(),
                            event.getOrderId(),
                            paymentId
                    );
                    String jsonRefund = objectMapper.writeValueAsString(refundEvent);

                    kafkaTemplate.send("payment-refunded", jsonRefund);
                    log.info("Sent PaymentRefundedEvent");
                } else {
                    log.warn("Payment not found for refund: {}", paymentId);
                }
            } else {
                log.warn("Saga mapping not found for refund: {}", event.getSagaId());
            }

        } catch (Exception e) {
            log.error("Failed to process refund", e);
        }
    }
}