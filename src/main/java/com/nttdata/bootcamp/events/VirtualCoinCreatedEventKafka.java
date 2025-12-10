package com.nttdata.bootcamp.events;

import com.nttdata.bootcamp.entity.dto.ChargeConsumptionKafkaDto;
import com.nttdata.bootcamp.entity.dto.VirtualCoinKafkaDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class VirtualCoinCreatedEventKafka extends EventKafka<VirtualCoinKafkaDto> {

}
