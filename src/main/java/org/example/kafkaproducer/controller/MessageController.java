package org.example.kafkaproducer.controller;

import org.example.kafkaproducer.Producer;
import org.example.kafkaproducer.Consumer; // Importa la clase Consumer
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessageController  {

    private final Producer kafkaProducer = Producer.getInstance();
    private final Consumer kafkaConsumer = Consumer.getInstance(); // Instancia el consumidor

    @CrossOrigin(origins = "http://localhost:4200")
    @PostMapping("/send")
    public void sendMessage(@RequestBody String message) {
        kafkaProducer.send(message);
    }

    @CrossOrigin(origins = "http://localhost:4200")
    @PostMapping("/consume")
    public void consumeMessages() {
        kafkaConsumer.start();
    }
}