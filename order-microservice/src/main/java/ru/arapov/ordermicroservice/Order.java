package ru.arapov.ordermicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder
public class Order {

    String id;
    String customerId;
    Double amount;
    String status;
    Map<String, Integer> items = new HashMap<>();
}
