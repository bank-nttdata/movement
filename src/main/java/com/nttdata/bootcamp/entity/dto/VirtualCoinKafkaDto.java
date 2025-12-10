package com.nttdata.bootcamp.entity.dto;

import lombok.Data;
import org.springframework.data.annotation.Id;

@Data
public class VirtualCoinKafkaDto {

    @Id
    private String id;

    private String dni;
    private Boolean flagDebitCard;
    private String numberDebitCard;
    private String numberAccount;
    private Double mount;


}
