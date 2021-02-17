package io.dmcapps.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.jboss.logging.Logger;

import io.dmcapps.proto.catalog.Product;
import io.dmcapps.proto.catalog.Product.Builder;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@Path("/products")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ProductController {

    private static final Logger LOGGER = Logger.getLogger("RequestController");

    @Inject
    @Channel("products")
    Emitter<Product> emitter;

    @POST
    public Response addProduct(String productRequest) {
        Builder productBuilder = Product.newBuilder();
        convertJSONToProtobuf(productRequest, productBuilder);
        productBuilder.setStatus(io.dmcapps.proto.catalog.Product.Status.PENDING);
        Product product = productBuilder.build();
        LOGGER.infof("Creating product %s", product.getName());
        emitter.send(KafkaRecord.of(null, product));
        return Response.accepted().build();
    }

    @PUT
    @Path("/{id}")
    public Response updateProduct(String productRequest, Long id) {
        Builder productBuilder = Product.newBuilder();
        convertJSONToProtobuf(productRequest, productBuilder);
        Product product = productBuilder
        .setId(id)
        .setStatus(io.dmcapps.proto.catalog.Product.Status.PENDING)
        .build();
        LOGGER.infof("Updating product %s", product.getName());
        emitter.send(KafkaRecord.of(product.getId(), product));
        return Response.accepted().build();
    }
    
    @DELETE
    @Path("/{id}")
    public Response delteProduct(String id) {
        LOGGER.infof("Deleting product %s", id);
        emitter.send(KafkaRecord.of(id, null));
        return Response.accepted().build();
    }
    
    private void convertJSONToProtobuf(String json, Message.Builder builder) {
        try {
            JsonFormat.parser().merge(json, builder);
        } catch (InvalidProtocolBufferException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}