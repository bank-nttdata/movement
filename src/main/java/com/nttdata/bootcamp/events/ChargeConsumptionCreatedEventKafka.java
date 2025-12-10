package com.nttdata.bootcamp.events;

import com.nttdata.bootcamp.entity.dto.ChargeConsumptionKafkaDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ChargeConsumptionCreatedEventKafka extends EventKafka<ChargeConsumptionKafkaDto> {

}
