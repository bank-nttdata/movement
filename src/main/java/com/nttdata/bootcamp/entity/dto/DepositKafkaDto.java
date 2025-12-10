package com.nttdata.bootcamp.entity.dto;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class DepositKafkaDto {

    @Id
    private String id;

    private String dni;
    private String accountNumber;
    private String typeAccount;

    private String depositNumber;
    private Double amount;
    private Double commission;
    private String status;

}
