package ru.arapov.paymentmicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderCreatedEvent implements Serializable {

    String sagaId;
    String orderId;
    String customerId;
    Double amount;
    Map<String, Integer> items = new HashMap<>();
}
