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
    MapReactiveUserDetailsService authentication() {
        return new MapReactiveUserDetailsService(
                User.withDefaultPasswordEncoder().username("jlong").password("pw").roles("USER").build());
    }

    @Bean
    PayloadSocketAcceptorInterceptor authorization(RSocketSecurity security) {
        return security
                .authorizePayload(ap -> ap.anyExchange().authenticated())
                .simpleAuthentication(Customizer.withDefaults())
                .build();
    }

    @Bean
    RSocketMessageHandler rSocketMessageHandler(RSocketStrategies strategies) {
        var rmh = new RSocketMessageHandler();
        rmh.getArgumentResolverConfigurer().addCustomResolver(new AuthenticationPrincipalArgumentResolver());
        rmh.setRSocketStrategies(strategies);
        return rmh;
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
            @AuthenticationPrincipal Mono<UserDetails> userDetailsMono) {
        return userDetailsMono
                .map(UserDetails::getUsername)
                .map(GreetingRequest::new)
                .flatMapMany(gr -> greet(clientRSocket, gr));
    }

    private Flux<GreetingResponse> greet(
            RSocketRequester clientRSocket,
            GreetingRequest request) {

        var out = Flux
                .fromStream(Stream.generate(() -> new GreetingResponse("Hello, " + request.getName() + " @ " + Instant.now())))
                .delayElements(Duration.ofSeconds(1))
                .take(100);

        var in = clientRSocket.route("health")
                .retrieveFlux(ClientHealthState.class)
                .filter(chs -> !chs.isHealthy())
                .doOnNext(chs -> log.info("got an unhealthy one!"));

        return out.takeUntilOther(in);
    }

}
