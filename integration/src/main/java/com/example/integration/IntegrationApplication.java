package com.example.integration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.rsocket.ClientRSocketConnector;
import org.springframework.integration.rsocket.RSocketInteractionModel;
import org.springframework.integration.rsocket.dsl.RSockets;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.rsocket.RSocketStrategies;

import java.io.File;

@Log4j2
@SpringBootApplication
public class IntegrationApplication {

    // adapter: unidirectional communication
    // outer service -> SI (inbound adapter)
    // SI -> outer service (outbound adapter)
    // SI -> outer service -> SI  (outbound gateway)
    // outer service -> SI -> outer service  (inbound gateway)
    // filters , splitters, aggregators, transformers, routers, etc.
    // channels
    // file | rsocket | log

    @Bean
    ClientRSocketConnector clientRSocketConnector(RSocketStrategies strategies) {
        var crc = new ClientRSocketConnector("localhost", 8888);
        crc.setRSocketStrategies(strategies);
        return crc;
    }

    @Bean
    MessageChannel fluxChannel() {
        return MessageChannels.flux().get();
    }

    @Bean
    IntegrationFlow rsocketFlow(
            ClientRSocketConnector crc,
            @Value("${user.home}") File home) {

        var inFolder = new File(new File(home, "Desktop"), "in");
        var fileInboundAdapter = Files
                .inboundAdapter(inFolder)
                .autoCreateDirectory(true);

        var rsocket = RSockets
                .outboundGateway("greetings")
                .clientRSocketConnector(crc)
                .expectedResponseType(GreetingResponse.class)
                .interactionModel(RSocketInteractionModel.requestStream);

        return IntegrationFlows
                .from(fileInboundAdapter, pmc -> pmc.poller(pm -> pm.fixedRate(1_000)))
                .transform(new FileToStringTransformer())
                .transform(String.class, GreetingRequest::new)
                .handle(rsocket)
                .split()
                .channel(fluxChannel())
                .handle((GenericHandler<GreetingResponse>) (greetingResponse, messageHeaders) -> {
                    log.info("new message : " + greetingResponse.toString());
                    messageHeaders.forEach((k, v) -> log.info(k + ':' + v));
                    return null;
                })
                .get();
    }


    public static void main(String[] args) {
        SpringApplication.run(IntegrationApplication.class, args);
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
