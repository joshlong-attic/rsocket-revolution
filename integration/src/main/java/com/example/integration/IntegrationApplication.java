package com.example.integration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.rsocket.ClientRSocketConnector;
import org.springframework.integration.rsocket.RSocketInteractionModel;
import org.springframework.integration.rsocket.dsl.RSockets;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
@SpringBootApplication
public class IntegrationApplication {

    @Bean
    MessageSource<String> namesMessageSource() {
        var names = "Jacky,Long,Juven,Mercy,Zhen,Zhouyoue,Jixi".split(",");
        var ctr = new AtomicInteger();
        return () -> {
            var next = ctr.getAndIncrement();
            while (next < names.length) {
                return MessageBuilder.withPayload(names[next]).build();
            }
            return null;
        };
    }

    @Bean
    MessageChannel channel() {
        return MessageChannels.flux().get();
    }

    @Bean
    ClientRSocketConnector clientRSocketConnector(RSocketStrategies strategies) {
        ClientRSocketConnector clientRSocketConnector = new ClientRSocketConnector("localhost", 8888);
        clientRSocketConnector.setRSocketStrategies(strategies);
        return clientRSocketConnector;
    }

    @Bean
    IntegrationFlow rsocketClientFlow(ClientRSocketConnector socketConnector) {
        return IntegrationFlows
                .from(namesMessageSource(), poller -> poller.poller(pm -> pm.fixedRate(1000)))
                .transform(String.class, GreetingRequest::new)// <5>
                .handle(RSockets//
                        .outboundGateway("greetings")// <6>
                        .interactionModel(RSocketInteractionModel.requestStream)//
                        .expectedResponseType(GreetingResponse.class)//
                        .clientRSocketConnector(socketConnector)//
                )//
                .split()// <7>
                .channel(this.channel()) // <8>
                .handle((GenericHandler<GreetingResponse>) (payload, headers) -> {// <9>
                    log.info("-----------------");
                    log.info(payload.toString());
                    headers.forEach((header, value) -> log.info(header + "=" + value));
                    return null;
                })//
                .get();
    }

    public static void main(String[] args) {
        SpringApplication.run(IntegrationApplication.class, args);
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

