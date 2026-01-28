package ru.arapov.paymentmicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PaymentProcessedEvent implements Serializable {

    String sagaId;
    String paymentId;
    String orderId;
    Double amount;
    boolean success;
    String message;
}
