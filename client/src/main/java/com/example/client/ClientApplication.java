package com.example.client;

import io.rsocket.SocketAcceptor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class ClientApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
        System.in.read();
    }

    private final String hostname = "localhost";
    private final int port = 8888;


    @Bean
    SocketAcceptor handler(RSocketStrategies strategies, HealthController healthController) {
        return RSocketMessageHandler.responder(strategies, healthController);
    }

    @Bean
    RSocketRequester rSocketRequester(SocketAcceptor handler, RSocketRequester.Builder builder) {
        return builder
                .rsocketConnector(connector -> connector.acceptor(handler))
                .connectTcp(this.hostname, this.port)
                .block();
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> client(RSocketRequester rSocketRequester) {
        return srgs -> {

            rSocketRequester
                    .route("greetings")
                    .data(new GreetingRequest("Alibaba"))
                    .retrieveFlux(GreetingResponse.class)
                    .subscribe(System.out::println);
        };
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

@Controller
class HealthController {

    @MessageMapping("health")
    Flux<ClientHealthState> clientHealthStateFlux() {
        return Flux
                .fromStream(Stream.generate(() -> new ClientHealthState(Math.random() < .8)))
                .delayElements(Duration.ofSeconds(1));
    }

}