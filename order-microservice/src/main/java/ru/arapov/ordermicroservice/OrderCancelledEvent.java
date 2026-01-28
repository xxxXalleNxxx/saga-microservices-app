package ru.arapov.ordermicroservice;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class OrderCancelledEvent implements Serializable {

    private String sagaId;
    private String orderId;
    private String reason;
}
