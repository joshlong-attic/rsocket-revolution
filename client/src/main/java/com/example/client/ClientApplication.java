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

    private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
    private final MimeType mimeType = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

    @Bean
    RSocketStrategiesCustomizer strategiesCustomizer() {
        return strategies -> strategies.encoder(new SimpleAuthenticationEncoder());
    }

    @Bean
    SocketAcceptor socketAcceptor(
            RSocketStrategies strategies,
            HealthController controller) {
        return RSocketMessageHandler.responder(strategies, controller);
    }

    @Bean
    RSocketRequester rSocketRequester(
            SocketAcceptor acceptor,
            RSocketRequester.Builder builder) {
        return builder
                .setupMetadata(this.credentials, this.mimeType)
                .rsocketConnector(connector -> connector.acceptor(acceptor))
                .connectTcp("localhost", 8888)
                .block();
    }

    @Bean
    ApplicationListener<ApplicationReadyEvent> client(RSocketRequester client) {
        return args ->
                client
                        .route("greetings")
                        .metadata(this.credentials, this.mimeType)
                        .data(Mono.empty())
//                        .data(new GreetingRequest("Alibaba"))
                        .retrieveFlux(GreetingResponse.class)
                        .subscribe(System.out::println);
    }

    @SneakyThrows
    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
        System.in.read();
    }

}

@Controller
class HealthController {

    @MessageMapping("health")
    Flux<ClientHealthState> health() {
        var stream = Stream.generate(() -> new ClientHealthState(Math.random() > .2));
        return Flux.fromStream(stream).delayElements(Duration.ofSeconds(1));
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
    private boolean healthy;
}

// DTO
@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingResponse {
    private String message;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class GreetingRequest {
    private String name;
}
