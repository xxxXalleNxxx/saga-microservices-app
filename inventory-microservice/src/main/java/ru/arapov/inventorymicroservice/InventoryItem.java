package ru.arapov.inventorymicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder
public class InventoryItem {

    String id;
    String name;
    Integer quantity;
    Integer reserved;
}
