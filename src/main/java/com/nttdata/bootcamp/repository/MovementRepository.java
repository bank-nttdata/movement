package com.nttdata.bootcamp.repository;

import com.nttdata.bootcamp.entity.Movement;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

//Mongodb Repository
public interface MovementRepository extends ReactiveCrudRepository<Movement, String> {
}
