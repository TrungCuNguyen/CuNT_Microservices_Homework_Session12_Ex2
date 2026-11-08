package com.example.emailservice.event;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class OrderCreatedEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long orderId;
    private String orderCode;
    private String customerEmail;
    private String customerName;
    private BigDecimal totalAmount;
    private List<OrderItemEvent> items;
    private Long timestamp;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @ToString
    public static class OrderItemEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        private Long productId;
        private String productName;
        private Integer quantity;
        private BigDecimal price;
    }
}
