package com.nttdata.bootcamp.events;

import com.nttdata.bootcamp.entity.dto.DepositKafkaDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DepositCreatedEventKafka extends EventKafka<DepositKafkaDto> {

}
