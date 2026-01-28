package ru.arapov.inventorymicroservice;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InventoryReleasedEvent {

    private String sagaId;
    private String orderId;
}
