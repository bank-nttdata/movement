package com.nttdata.bootcamp.service.impl;

import com.nttdata.bootcamp.entity.Movement;
import com.nttdata.bootcamp.entity.dto.*;
import com.nttdata.bootcamp.entity.enums.EventType;
import com.nttdata.bootcamp.events.*;
import com.nttdata.bootcamp.repository.MovementRepository;
import com.nttdata.bootcamp.service.KafkaService;
import com.nttdata.bootcamp.service.MovementService;
import com.nttdata.bootcamp.util.Constant;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.Date;
import java.util.UUID;

@Slf4j
@Service
public class KafkaServiceImpl implements KafkaService {

    private final MovementRepository movementRepository;
    private final MovementService movementService;
    private final KafkaSender<String, EventKafka<?>> reactiveSender;

    @Value("${topic.movement.name}")
    private String topicMovement;

    public KafkaServiceImpl(
            MovementRepository movementRepository,
            MovementService movementService,
            KafkaSender<String, EventKafka<?>> reactiveSender) {

        this.movementRepository = movementRepository;
        this.movementService = movementService;
        this.reactiveSender = reactiveSender;
    }

    // ==========================
    // PUBLICAR REACTIVO
    // ==========================
    public Mono<Void> publishReactive(Movement movement) {

        MovementCreatedEventKafka event = new MovementCreatedEventKafka();
        event.setId(UUID.randomUUID().toString());
        event.setDate(new Date());
        event.setType(EventType.CREATED);
        event.setData(movement);

        SenderRecord<String, EventKafka<?>, Void> record =
                SenderRecord.create(topicMovement, null, null, event.getId(), event, null);

        return reactiveSender.send(Mono.just(record))
                .doOnNext(res -> log.info("✔ Enviado a Kafka: {}", movement.getMovementNumber()))
                .then();
    }

    // ==========================
    // GUARDAR REACTIVO
    // ==========================
    @Override
    public Mono<Void> saveMovementReactive(Movement movement) {
        return movementRepository.save(movement)
                .doOnNext(m -> log.info("✔ Movimiento guardado: {}", m.getMovementNumber()))
                .flatMap(this::publishReactive);
    }


    // ==========================
    // HANDLER GENERAL
    // ==========================

    private Mono<Void> handleDeposit(DepositKafkaDto dto) {

        return movementService.findByAccountNumber(dto.getAccountNumber())
                .count()
                .map(count -> count > Constant.COUNT_TRANSACTIONS ?
                        Constant.COMMISSION_TRANSACTIONS :
                        dto.getCommission())
                .flatMap(comm -> {

                    Movement movement = new Movement();
                    movement.setCommission(comm);
                    movement.setDni(dto.getDni());
                    movement.setMovementNumber(dto.getDepositNumber());
                    movement.setAccountNumber(dto.getAccountNumber());
                    movement.setAmount(dto.getAmount());
                    movement.setTypeTransaction("DEPOSIT");
                    movement.setCreationDate(new Date());
                    movement.setModificationDate(new Date());
                    movement.setStatus(Constant.STATUS);

                    return saveMovementReactive(movement);
                });
    }

    private Mono<Void> handleWithdrawal(WithdrawalKafkaDto dto) {

        return movementService.findByAccountNumber(dto.getAccountNumber())
                .count()
                .map(count -> count > Constant.COUNT_TRANSACTIONS ?
                        Constant.COMMISSION_TRANSACTIONS :
                        dto.getCommission())
                .flatMap(comm -> {

                    Movement movement = new Movement();
                    movement.setCommission(comm);
                    movement.setDni(dto.getDni());
                    movement.setMovementNumber(dto.getWithdrawalNumber());
                    movement.setAccountNumber(dto.getAccountNumber());
                    movement.setAmount(dto.getAmount() * -1);
                    movement.setTypeTransaction("WITHDRAWAL");
                    movement.setCreationDate(new Date());
                    movement.setModificationDate(new Date());
                    movement.setStatus(Constant.STATUS);

                    return saveMovementReactive(movement);
                });
    }

    private Mono<Void> handlePayment(PaymentKafkaDto dto) {

        Movement movement = new Movement();
        movement.setDni(dto.getDni());
        movement.setMovementNumber(dto.getPaymentNumber());
        movement.setAccountNumber(dto.getAccountNumber());
        movement.setAmount(dto.getAmount());
        movement.setCommission(dto.getCommission());
        movement.setTypeTransaction("PAYMENT");
        movement.setCreationDate(new Date());
        movement.setModificationDate(new Date());
        movement.setStatus(Constant.STATUS);

        return saveMovementReactive(movement);
    }

    private Mono<Void> handleCharge(ChargeConsumptionKafkaDto dto) {

        Movement movement = new Movement();
        movement.setDni(dto.getDni());
        movement.setMovementNumber(dto.getChargeNumber());
        movement.setAccountNumber(dto.getAccountNumber());
        movement.setAmount(dto.getAmount() * -1);
        movement.setCommission(dto.getCommission());
        movement.setTypeTransaction("CHARGE");
        movement.setCreationDate(new Date());
        movement.setModificationDate(new Date());
        movement.setStatus(Constant.STATUS);

        return saveMovementReactive(movement);
    }

    private Mono<Void> handleVirtualCoin(VirtualCoinKafkaDto dto) {

        if (!dto.getFlagDebitCard()) return Mono.empty();

        Movement movement = new Movement();
        movement.setDni(dto.getDni());
        movement.setAccountNumber(dto.getNumberAccount());
        movement.setMovementNumber(UUID.randomUUID().toString());
        movement.setAmount(dto.getMount());
        movement.setCommission(0.00);
        movement.setTypeTransaction("VIRTUAL_COIN");
        movement.setCreationDate(new Date());
        movement.setModificationDate(new Date());
        movement.setStatus(Constant.STATUS);

        return saveMovementReactive(movement);
    }

    // ==========================
    // LISTENERS REACTIVOS
    // ==========================

    @KafkaListener(
            topics = "${topic.customer.name:topic_deposit}",
            groupId = "grupo1")
    public void onDeposit(EventKafka<?> event) {
        if (event instanceof DepositCreatedEventKafka) {
            DepositCreatedEventKafka d = (DepositCreatedEventKafka) event;
            log.info("Received deposit event id={} data={}",
                    d.getId(), d.getData());
            // EXTRAER EL DTO CORRECTO
            DepositKafkaDto dto = d.getData();
            // LLAMAR AL HANDLER CON EL DTO CORRECTO
            handleDeposit(dto).subscribe();
        }
    }


    @KafkaListener(topics = "${topic.customer.name:topic_withdrawal}", groupId = "grupo1")
    public void onWithdrawal(EventKafka<?> event) {
        if (event instanceof WithdrawalCreatedEventKafka) {
            WithdrawalCreatedEventKafka w = (WithdrawalCreatedEventKafka) event;
            log.info("Withdrawal recibido: {}", w.getData());
            WithdrawalKafkaDto dto = w.getData();
            handleWithdrawal(dto).subscribe();
        }
    }


    @KafkaListener(topics = "${topic.customer.name:topic_payment}", groupId = "grupo1")
    public void onPayment(EventKafka<?> event) {
        if (event instanceof PaymentCreatedEventKafka) {
            PaymentCreatedEventKafka p = (PaymentCreatedEventKafka) event;
            log.info("Payment recibido: {}", p.getData());
            PaymentKafkaDto dto = p.getData();
            handlePayment(dto).subscribe();
        }
    }


    @KafkaListener(topics = "${topic.customer.name:topic_charge}", groupId = "grupo1")
    public void onCharge(EventKafka<?> event) {

        if (event instanceof ChargeConsumptionCreatedEventKafka) {
            ChargeConsumptionCreatedEventKafka c = (ChargeConsumptionCreatedEventKafka) event;
            log.info("Charge recibido: {}", c.getData());
            ChargeConsumptionKafkaDto dto = c.getData();
            handleCharge(dto).subscribe();
        }
    }

    @KafkaListener(topics = "${topic.customer.name:topic_virtualCoin}", groupId = "grupo1")
    public void onVirtualCoin(EventKafka<?> event) {
        if (event instanceof VirtualCoinCreatedEventKafka) {
            VirtualCoinCreatedEventKafka v = (VirtualCoinCreatedEventKafka) event;
            log.info("VirtualCoin recibido: {}", v.getData());
            VirtualCoinKafkaDto dto = v.getData();
            handleVirtualCoin(dto).subscribe();
        }
    }

}
