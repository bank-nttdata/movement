package com.nttdata.bootcamp.entity.dto;

import lombok.Data;
import org.springframework.data.annotation.Id;
@Data
public class PaymentKafkaDto {
    @Id
    private String id;

    private String dni;
    private String accountNumber;
    private String typeAccount;

    private String paymentNumber;
    private Double amount;
    private Double commission;
    private String status;
}
