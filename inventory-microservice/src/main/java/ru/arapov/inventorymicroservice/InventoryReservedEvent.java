package ru.arapov.inventorymicroservice;

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
public class InventoryReservedEvent implements Serializable {

    String sagaId;
    String orderId;
    Map<String, Integer> reservedItems = new HashMap<>();
    boolean success;
    String message;
}
