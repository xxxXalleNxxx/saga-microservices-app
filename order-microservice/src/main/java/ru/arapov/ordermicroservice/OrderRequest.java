package ru.arapov.ordermicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderRequest {

    String customerId;
    Double amount;
    Map<String, Integer> items = new HashMap<>();
}
