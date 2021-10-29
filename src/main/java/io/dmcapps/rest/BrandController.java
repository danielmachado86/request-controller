package io.dmcapps.rest;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import io.dmcapps.proto.catalog.Transaction;
import io.dmcapps.proto.catalog.Transaction.Builder;
import io.dmcapps.proto.catalog.Transaction.Type;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@Path("/brands")
public class BrandController {

    private static final Logger LOGGER = Logger.getLogger("RequestController");

    @Inject
    @Channel("product-catalog-transactions")
    Emitter<Transaction> emitter;

    @POST
    public Uni<Response> addBrand(String request) {
        final String uuid = UUID.randomUUID().toString().replace("-", "");
        Builder transactionBuilder = Transaction.newBuilder();
        transactionBuilder.setId(uuid).setTypeValue(Type.CREATE_VALUE).setEntityName("Brand").setPayload(request);
        Transaction transaction = transactionBuilder.build();
        LOGGER.infof("Transaction id: %s", transaction.getId());
        return Uni.createFrom().completionStage(emitter.send(transaction))
            .onItem().transformToUni(c -> Uni.createFrom().item(Response.accepted().link("http://localhost:9200/transactions/_doc/" + uuid, "canonical").build()));
    }

    @PUT
    @Path("/{name}")
    public Uni<Response> updateBrand(String request, String name) {
        final String uuid = UUID.randomUUID().toString().replace("-", "");
        Builder transactionBuilder = Transaction.newBuilder();
        transactionBuilder.setId(uuid).setTypeValue(Type.CREATE_VALUE).setEntityName("Brand").setPayload(request);
        Transaction transaction = transactionBuilder.build();
        LOGGER.infof("Transaction id: %s", transaction.getId());
        emitter.send(KafkaRecord.of(name, transaction));
        return Uni.createFrom().item(Response.accepted().link("http://localhost:9200/transactions/_doc/" + uuid, "canonical").build());
    }

    // @DELETE
    // @Path("/{name}")
    // public Response delteBrand(@PathParam("name") String name) {
    //     LOGGER.infof("Deleting brand %s", name);
    //     emitter.send(KafkaRecord.of(name, null));
    //     return Response.accepted().build();
    // }

    // private void convertJSONToProtobuf(String json, Message.Builder builder) {
    //     try {
    //         JsonFormat.parser().merge(json, builder);
    //     } catch (InvalidProtocolBufferException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }
    // }

}