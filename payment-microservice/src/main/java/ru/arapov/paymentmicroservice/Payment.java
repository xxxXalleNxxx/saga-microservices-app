package ru.arapov.paymentmicroservice;

import lombok.*;
import lombok.experimental.FieldDefaults;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder
public class Payment {

    String id;
    String orderId;
    Double amount;
    String status;
}
