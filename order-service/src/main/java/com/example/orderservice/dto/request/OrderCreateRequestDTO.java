package com.example.orderservice.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.*;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCreateRequestDTO {

    @NotBlank(message = "Email khách hàng không được để trống")
    @Email(message = "Email khách hàng không đúng định dạng")
    private String customerEmail;

    @NotBlank(message = "Tên khách hàng không được để trống")
    private String customerName;

    @NotEmpty(message = "Danh sách sản phẩm không được rỗng")
    @Valid
    private List<OrderItemRequestDTO> items;
}
