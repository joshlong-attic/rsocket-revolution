package com.example.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.messaging.handler.invocation.reactive.AuthenticationPrincipalArgumentResolver;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

@SpringBootApplication
public class ServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceApplication.class, args);
    }

}


@Configuration
class SecurityConfiguration {


    @Bean
    PayloadSocketAcceptorInterceptor interceptor(RSocketSecurity security) {
        return security
                .simpleAuthentication(Customizer.withDefaults())
                .authorizePayload(ap -> ap.anyExchange().authenticated())
                .build();
    }

    @Bean
    MapReactiveUserDetailsService authentication() {
        return new MapReactiveUserDetailsService(User.withDefaultPasswordEncoder().username("jlong").password("pw").roles("USER").build());
    }

    @Bean
    RSocketMessageHandler messageHandler(RSocketStrategies strategies) {
        var rmh = new RSocketMessageHandler();
        rmh.getArgumentResolverConfigurer().addCustomResolver(new AuthenticationPrincipalArgumentResolver());
        rmh.setRSocketStrategies(strategies);
        return rmh;
    }
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

@Controller
@Log4j2
class GreetingController {

    @MessageMapping("greetings")
    Flux<GreetingResponse> greet(
            RSocketRequester clientRSocketConnection,
            @AuthenticationPrincipal Mono<UserDetails> user) {
        return user
                .map(UserDetails::getUsername)
                .map(GreetingRequest::new)
                .flatMapMany(gr -> this.greet(clientRSocketConnection, gr));
    }

    private Flux<GreetingResponse> greet(
            RSocketRequester clientRSocketConnection, GreetingRequest requests) {

        var clientHealth = clientRSocketConnection
                .route("health")
                .retrieveFlux(ClientHealthState.class)
                .filter(chs -> !chs.isHealthy())
                .doOnNext(chs -> log.info("not healthy! "));

        var greetings = Flux
                .fromStream(Stream
                        .generate(() -> new GreetingResponse("ni hao " + requests.getName() + " @ " + Instant.now() + "!")))
                .take(100)
                .delayElements(Duration.ofSeconds(1));

        return greetings.takeUntilOther(clientHealth);
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
    private boolean healthy;
}