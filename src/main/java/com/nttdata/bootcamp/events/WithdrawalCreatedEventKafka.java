package com.nttdata.bootcamp.events;

import com.nttdata.bootcamp.entity.dto.WithdrawalKafkaDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class WithdrawalCreatedEventKafka extends EventKafka<WithdrawalKafkaDto> {

}
