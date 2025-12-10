package com.nttdata.bootcamp.entity.dto;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class WithdrawalKafkaDto {
    @Id
    private String id;

    private String dni;
    private String accountNumber;
    private String typeAccount;

    private String withdrawalNumber;
    private Double amount;
    private Double commission;
    private String status;

}
