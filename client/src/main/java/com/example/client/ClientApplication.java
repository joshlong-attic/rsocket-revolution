package com.example.client;

import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
public class ClientApplication {

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
        System.in.read();
    }

    // client
    private final String hostname = "localhost";
    private final int port = 8888;

    private final UsernamePasswordMetadata credentials =
            new UsernamePasswordMetadata("jlong", "pw");
    private final MimeType mimeType =
            MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

    @Bean
    RSocketStrategiesCustomizer rSocketStrategiesCustomizer() {
        return strategies -> strategies.encoder(new SimpleAuthenticationEncoder());
    }

    @Bean
    SocketAcceptor handler(RSocketStrategies strategies, HealthController healthController) {
        return RSocketMessageHandler.responder(strategies, healthController);
    }

    @Bean
    RSocketRequester rSocketRequester(SocketAcceptor handler, RSocketRequester.Builder builder) {
        return builder
                .setupMetadata(this.credentials, this.mimeType)
                .rsocketConnector(connector -> connector.acceptor(handler))
                .connectTcp(this.hostname, this.port)
                .block();
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> client(RSocketRequester rSocketRequester) {
        return srgs ->
                rSocketRequester
                        .route("greetings")
                        .metadata(this.credentials, this.mimeType)
                        .data(Mono.empty())
      //                .data(new GreetingRequest("Alibaba"))
                        .retrieveFlux(GreetingResponse.class)
                        .subscribe(System.out::println);
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