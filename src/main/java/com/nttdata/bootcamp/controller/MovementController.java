package com.nttdata.bootcamp.controller;

import com.nttdata.bootcamp.entity.Movement;
import com.nttdata.bootcamp.entity.dto.MovementDto;
import com.nttdata.bootcamp.service.MovementService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.validation.Valid;
import java.util.Date;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/movement")
public class MovementController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MovementController.class);

    private final MovementService movementService;

    public MovementController(MovementService movementService) {
        this.movementService = movementService;
    }

    // ============================
    // FIND ALL
    // ============================
    @GetMapping("/findAllMovements")
    public Flux<Movement> findAllMovements() {
        return movementService.findAll()
                .doOnSubscribe(s -> LOGGER.info("Searching all movements"))
                .doOnNext(m -> LOGGER.info("Movement: {}", m));
    }

    // ============================
    // FIND BY ACCOUNT NUMBER
    // ============================
    @GetMapping("/findAllMovementsByNumber/{accountNumber}")
    public Flux<Movement> findAllMovementsByNumber(@PathVariable String accountNumber) {
        return movementService.findByAccountNumber(accountNumber)
                .doOnSubscribe(s -> LOGGER.info("Searching movements for account {}", accountNumber))
                .doOnNext(m -> LOGGER.info("Movement found: {}", m));
    }

    // ============================
    // FIND BY MOVEMENT NUMBER
    // ============================
    @CircuitBreaker(name = "movement", fallbackMethod = "fallBackMovement")
    @GetMapping("/findByMovementNumber/{numberMovement}")
    public Mono<Movement> findByMovementNumber(@PathVariable String numberMovement) {
        LOGGER.info("Searching movement {}", numberMovement);
        return movementService.findByNumber(numberMovement);
    }

    // ============================
    // UPDATE COMMISSION
    // ============================
    @CircuitBreaker(name = "movement", fallbackMethod = "fallBackMovement")
    @PutMapping("/updateCommission/{numberMovement}/{commission}")
    public Mono<Movement> updateCommission(
            @PathVariable String numberMovement,
            @PathVariable Double commission) {

        LOGGER.info("Updating commission for movement {}", numberMovement);

        return movementService.findByNumber(numberMovement)
                .switchIfEmpty(Mono.error(new RuntimeException("Movement not found")))
                .flatMap(existing -> {
                    existing.setCommission(commission);
                    existing.setModificationDate(new Date());
                    return movementService.saveMovement(existing);
                });
    }

    // ============================
    // SAVE ORIGIN
    // ============================
    @PostMapping("/saveTransactionOrigin")
    public Mono<Movement> saveTransactionOrigin(@RequestBody Movement movement) {

        movement.setCreationDate(new Date());
        movement.setModificationDate(new Date());
        movement.setTypeTransaction("Transfer");
//        movement.setFlagDebit(true); VERIFICAR SI VA A IR
//        movement.setFlagCredit(false); VERIFICAR SI VA A IR

        LOGGER.info("Saving transfer origin {}", movement);

        return movementService.saveMovement(movement);
    }

    // ============================
    // SAVE DESTINATION
    // ============================
    @PostMapping("/saveTransactionDestination")
    public Mono<Movement> saveTransactionDestination(@RequestBody Movement movement) {

        movement.setCreationDate(new Date());
        movement.setModificationDate(new Date());
        movement.setTypeTransaction("Transfer");
//        movement.setFlagDebit(false); VERIFICAR
//        movement.setFlagCredit(true); VERIFICAR

        LOGGER.info("Saving transfer destination {}", movement);

        return movementService.saveMovement(movement);
    }

    // ============================
    // UPDATE MOVEMENT
    // ============================
    @CircuitBreaker(name = "movement", fallbackMethod = "fallBackMovement")
    @PutMapping("/updateMovement/{numberMovement}")
    public Mono<Movement> updateMovement(
            @PathVariable String numberMovement,
            @Valid @RequestBody Movement movement) {

        LOGGER.info("Updating movement {}", numberMovement);

        return movementService.findByNumber(numberMovement)
                .switchIfEmpty(Mono.error(new RuntimeException("Movement not found")))
                .flatMap(existing -> {
                    movement.setMovementNumber(numberMovement);
                    movement.setCreationDate(existing.getCreationDate());
                    movement.setModificationDate(new Date());
                    return movementService.updateMovement(movement);
                });
    }

    // ============================
    // DELETE
    // ============================
    @CircuitBreaker(name = "movement", fallbackMethod = "fallBackVoid")
    @DeleteMapping("/deleteMovement/{numberMovement}")
    public Mono<Void> deleteMovement(@PathVariable String numberMovement) {
        LOGGER.info("Deleting movement {}", numberMovement);
        return movementService.deleteMovement(numberMovement);
    }


    // ============================
    // FALLBACKS
    // ============================
    private Mono<Movement> fallBackMovement(String param, Throwable ex) {
        LOGGER.error("Fallback triggered for {}, error={}", param, ex.toString());
        return Mono.just(new Movement());
    }

    private Mono<Void> fallBackVoid(String param, Throwable ex) {
        LOGGER.error("Fallback VOID triggered for {}, error={}", param, ex.toString());
        return Mono.empty();
    }
}
