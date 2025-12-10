package com.nttdata.bootcamp.controller;

import com.nttdata.bootcamp.entity.Movement;
import com.nttdata.bootcamp.entity.dto.MovementDto;
import com.nttdata.bootcamp.service.MovementService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/report")
public class ReportController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportController.class);

    private final MovementService movementService;

    public ReportController(MovementService movementService) {
        this.movementService = movementService;
    }

    // ============================
    // PARSE FECHA REACTIVA
    // ============================
    private Mono<Date> parseDate(String date) {
        return Mono.fromCallable(() -> {
            try {
                return new SimpleDateFormat("dd-MM-yyyy").parse(date);
            } catch (ParseException e) {
                throw new RuntimeException("Invalid date format: " + date);
            }
        });
    }

    private MovementDto buildDto(Movement m) {
        return new MovementDto(
                m.getDni(),
                m.getAccountNumber(),
                m.getMovementNumber(),
                m.getAmount()
        );
    }

    // ============================
    // GET COMMISSIONS BY DATE RANGE
    // ============================
    @GetMapping("/getCommissionsByAccount/{accountNumber}/{date1}/{date2}")
    public Flux<MovementDto> getCommissionsByAccount(
            @PathVariable String accountNumber,
            @PathVariable String date1,
            @PathVariable String date2) {

        return Mono.zip(parseDate(date1), parseDate(date2))
                .flatMapMany(dates -> {
                    Date start = dates.getT1();
                    Date end = dates.getT2();

                    LOGGER.info("Searching commissions for account {} between {} and {}",
                            accountNumber, date1, date2);

                    return movementService.findCommissionByAccountNumber(accountNumber)
                            .filter(m -> m.getCreationDate().after(start)
                                    && m.getCreationDate().before(end))
                            .map(this::buildDto);
                });
    }

    // ============================
    // REPORT GENERAL OF MOVEMENTS
    // ============================
    @GetMapping("/getReportByProduct/{accountNumber}/{date1}/{date2}")
    public Flux<MovementDto> getReportByProduct(
            @PathVariable String accountNumber,
            @PathVariable String date1,
            @PathVariable String date2) {

        return Mono.zip(parseDate(date1), parseDate(date2))
                .flatMapMany(dates -> {
                    Date start = dates.getT1();
                    Date end = dates.getT2();

                    LOGGER.info("Searching movements for product {} between {} and {}",
                            accountNumber, date1, date2);

                    return movementService.findByAccountNumber(accountNumber)
                            .filter(m -> m.getCreationDate().after(start)
                                    && m.getCreationDate().before(end))
                            .map(this::buildDto);
                });
    }

    // ============================
    // TOP 10 MOVEMENTS
    // ============================
    @GetMapping("/findTopMovements/{accountNumber}")
    public Flux<MovementDto> findTopMovements(@PathVariable String accountNumber) {

        LOGGER.info("Searching top movements for account {}", accountNumber);

        return movementService.findByAccountNumber(accountNumber)
                .map(this::buildDto)
                .take(10); // take es REACTIVO, NO bloquea
    }
}
