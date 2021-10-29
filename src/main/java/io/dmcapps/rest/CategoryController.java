package io.dmcapps.rest;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.POST;
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

@Path("/categories")
public class CategoryController {

    private static final Logger LOGGER = Logger.getLogger("RequestController");

    @Inject
    @Channel("product-catalog-transactions")
    Emitter<Transaction> emitter;

    @POST
    public Uni<Response> addCategory(String request) {
        final String uuid = UUID.randomUUID().toString().replace("-", "");
        Builder transactionBuilder = Transaction.newBuilder();
        transactionBuilder.setId(uuid).setTypeValue(Type.CREATE_VALUE).setEntityName("Category").setPayload(request);
        Transaction transaction = transactionBuilder.build();
        LOGGER.infof("Transaction id: %s", transaction.getId());
        return Uni.createFrom().completionStage(emitter.send(transaction))
            .onItem().transformToUni(c -> Uni.createFrom().item(Response.accepted().link("http://localhost:9200/transactions/_doc/" + uuid, "canonical").build()));
    }

    // @PUT
    // @Path("/parent/{parent}/name/{name}")
    // public Response updateCategory(String categoryRequest, String parent, String name) {
    //     Builder categoryBuilder = Category.newBuilder();
    //     convertJSONToProtobuf(categoryRequest, categoryBuilder);
    //     Category category = categoryBuilder
    //         .setParent(parent)
    //         .setName(name)
    //         .setStatus(io.dmcapps.proto.catalog.Category.Status.PENDING)
    //         .build();
    //     LOGGER.infof("Updating category %s", category.getName());
    //     emitter.send(KafkaRecord.of(category.getId(), category));
    //     return Response.accepted().build();
    // }

    // @DELETE
    // @Path("/parent/{parent}/name/{name}")
    // public Response delteCategory(@PathParam("parent") String parent, @PathParam("name") String name) {
    //     LOGGER.infof("Deleting category %s", name + ":" + parent);
    //     emitter.send(KafkaRecord.of(name + ":" + parent, null));
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