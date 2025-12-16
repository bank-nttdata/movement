package com.nttdata.bootcamp.service.impl;

import com.nttdata.bootcamp.entity.Movement;
import com.nttdata.bootcamp.repository.MovementRepository;
import com.nttdata.bootcamp.service.MovementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

//Service implementation
@Service
public class MovementServiceImpl implements MovementService {

    @Autowired
    private MovementRepository movementRepository;

    // ============================================
    // FIND ALL
    // ============================================
    @Override
    public Flux<Movement> findAll() {
        return movementRepository.findAll();
    }

    // ============================================
    // FIND BY ACCOUNT NUMBER
    // ============================================
    @Override
    public Flux<Movement> findByAccountNumber(String accountNumber) {
        return movementRepository
                .findAll()
                .filter(m -> accountNumber.equals(m.getAccountNumber()));
    }

    // ============================================
    // FIND COMMISSIONS
    // ============================================
    @Override
    public Flux<Movement> findCommissionByAccountNumber(String accountNumber) {
        return movementRepository
                .findAll()
                .filter(m ->
                        accountNumber.equals(m.getAccountNumber())
                                && m.getCommission() > 0
                );
    }

    // ============================================
    // FIND BY MOVEMENT NUMBER
    // ============================================
    @Override
    public Mono<Movement> findByNumber(String number) {
        return movementRepository
                .findAll()
                .filter(m -> number.equals(m.getMovementNumber()))
                .next();
    }

    // ============================================
    // SAVE MOVEMENT
    // ============================================
    @Override
    public Mono<Movement> saveMovement(Movement movement) {
        movement.setStatus("active");
        return movementRepository.save(movement);
    }

    // ============================================
    // UPDATE MOVEMENT
    // ============================================
    @Override
    public Mono<Movement> updateMovement(Movement dataMovement) {

        return findByNumber(dataMovement.getMovementNumber())
                .switchIfEmpty(
                        Mono.error(new RuntimeException(
                                "The movement " + dataMovement.getMovementNumber() + " does not exist"
                        ))
                )
                .flatMap(existing -> {
                    // Copiar valores NO editables desde el registro original
                    dataMovement.setDni(existing.getDni());
                    dataMovement.setAmount(existing.getAmount());
                    dataMovement.setAccountNumber(existing.getAccountNumber());
                    dataMovement.setMovementNumber(existing.getMovementNumber());
                    dataMovement.setTypeTransaction(existing.getTypeTransaction());
                    dataMovement.setStatus(existing.getStatus());
                    dataMovement.setCreationDate(existing.getCreationDate());

                    return movementRepository.save(dataMovement);
                });
    }

    // ============================================
    // DELETE MOVEMENT
    // ============================================
    @Override
    public Mono<Void> deleteMovement(String number) {

        return findByNumber(number)
                .switchIfEmpty(
                        Mono.error(new RuntimeException(
                                "The movement number " + number + " does not exist"
                        ))
                )
                .flatMap(movementRepository::delete);
    }
}
