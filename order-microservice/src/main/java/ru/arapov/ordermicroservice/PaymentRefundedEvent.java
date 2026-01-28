package ru.arapov.ordermicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PACKAGE)
public class PaymentRefundedEvent implements Serializable {

    String sagaId;
    String paymentId;
    String orderId;
}
