//package com.nttdata.bootcamp.service;
//
//import com.nttdata.bootcamp.events.EventKafka;
//import com.nttdata.bootcamp.entity.Movement;
//
//public interface KafkaService {
//    void consumerDepositSave(EventKafka<?> eventKafka);
//    void consumerPaymentSave(EventKafka<?> eventKafka);
//    void consumerWithdrawalSave(EventKafka<?> eventKafka);
//    void consumerChargeSave(EventKafka<?> eventKafka);
//
//    void publish(Movement customer);
//}


package com.nttdata.bootcamp.service;

import com.nttdata.bootcamp.entity.Movement;
import reactor.core.publisher.Mono;

public interface KafkaService {

    /**
     * Publica un movimiento en Kafka de manera REACTIVA.
     * @param movement Movimiento a publicar
     * @return Mono<Void> completado cuando el envío finaliza
     */
    Mono<Void> publishReactive(Movement movement);

    /**
     * Guarda un movimiento en Mongo y luego lo publica a Kafka.
     * @param movement Movimiento a procesar
     * @return Mono<Void> completado cuando todo el flujo termina
     */
    Mono<Void> saveMovementReactive(Movement movement);
}
