package com.example.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

@SpringBootApplication
public class ServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingRequest {
    private String name;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingResponse {
    private String message;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
    private boolean healthy;
}


@Log4j2
@Controller
class GreetingController {

    @MessageMapping("greetings")
    Flux<GreetingResponse> greet(
            RSocketRequester clientRSocket,
            GreetingRequest request) {


        var out = Flux
                .fromStream(Stream.generate(() -> new GreetingResponse("Hello, " + request.getName() + " @ " + Instant.now())))
                .delayElements(Duration.ofSeconds(1))
                .take(100);

        var in = clientRSocket.route("health")
                .retrieveFlux(ClientHealthState.class)
                .filter(chs -> !chs.isHealthy())
                .doOnNext( chs ->  log.info ("got an unhealthy one!"));

        return out.takeUntilOther(in);
    }

}
