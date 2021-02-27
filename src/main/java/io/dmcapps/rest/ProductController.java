package io.dmcapps.rest;

import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.dmcapps.proto.catalog.Transaction;
import io.dmcapps.proto.catalog.Transaction.Builder;
import io.dmcapps.proto.catalog.Transaction.Type;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@Path("/products")
public class ProductController {

    private static final Logger LOGGER = Logger.getLogger("ProductController");

    @Inject
    @Channel("product-catalog-transactions")
    Emitter<Transaction> emitter;

    @POST
    public Uni<Response> addBrand(String request) {
        final String uuid = UUID.randomUUID().toString().replace("-", "");
        Builder transactionBuilder = Transaction.newBuilder();
        transactionBuilder.setId(uuid).setTypeValue(Type.CREATE_VALUE).setEntityName("Product").setPayload(request);
        Transaction transaction = transactionBuilder.build();
        LOGGER.infof("Transaction id: %s", transaction.getId());
        return Uni.createFrom().completionStage(emitter.send(transaction))
            .onItem().transformToUni(c -> Uni.createFrom().item(Response.accepted().link("http://localhost:9200/transactions/_doc/" + uuid, "canonical").build()));
        
    }
    
    // @PUT
    // @Path("/{id}")
    // public Response updateProduct(String productRequest, Long id) {
    //     Builder productBuilder = Product.newBuilder();
    //     mergeJSONAndProtobuf(productRequest, productBuilder);
    //     Product product = productBuilder
    //     .setId(id)
    //     .setStatus(io.dmcapps.proto.catalog.Product.Status.PENDING)
    //     .build();
    //     LOGGER.infof("Updating product %s", product.getName());
    //     emitter.send(KafkaRecord.of(product.getId(), product));
    //     return Response.accepted().build();
    // }
    
    // @DELETE
    // @Path("/{id}")
    // public Response delteProduct(String id) {
    //     LOGGER.infof("Deleting product %s", id);
    //     emitter.send(KafkaRecord.of(id, null));
    //     return Response.accepted().build();
    // }
    
}