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
    public Response addProduct(String productRequest) throws InvalidProtocolBufferException {
        Builder pBuilder = Product.newBuilder();
        JsonFormat.parser().merge(productRequest, pBuilder);
        pBuilder.setStatus(io.dmcapps.proto.catalog.Product.Status.PENDING);
        Product product = pBuilder.build();
        if (!product.getId().isEmpty()) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        LOGGER.infof("Creating product %s", product.getName());
        emitter.send(KafkaRecord.of(null, product));
        return Response.accepted().build();
    }

    @PUT
    @Path("/{id}")
    public Response updateProduct(String productRequest, String id) throws InvalidProtocolBufferException {
        Builder pBuilder = Product.newBuilder();
        JsonFormat.parser().merge(productRequest, pBuilder);
        Product product = pBuilder
            .setId(id)
            .setStatus(io.dmcapps.proto.catalog.Product.Status.PENDING)
            .build();
        LOGGER.infof("Updating product %s", product.getName());
        emitter.send(KafkaRecord.of(product.getId(), product));
        return Response.accepted().build();
    }

    @DELETE
    @Path("/{id}")
    public Response delteProduct(String productRequest, String id) {
        LOGGER.infof("Deleting product %s", id);
        emitter.send(KafkaRecord.of(id, null));
        return Response.accepted().build();
    }

}