package io.dmcapps.rest;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.jboss.logging.Logger;

import io.dmcapps.proto.catalog.Category;
import io.dmcapps.proto.catalog.Category.Builder;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@Path("/categories")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CategoryController {

    private static final Logger LOGGER = Logger.getLogger("RequestController");

    @Inject
    @Channel("categories")
    Emitter<Category> emitter;

    @POST
    public Response addCategory(String categoryRequest) {
        Builder categoryBuilder = Category.newBuilder();
        convertJSONToProtobuf(categoryRequest, categoryBuilder);
        categoryBuilder.setStatus(io.dmcapps.proto.catalog.Category.Status.PENDING);
        Category category = categoryBuilder.build();
        if (category.getName().isEmpty() || category.getParent().isEmpty()) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        LOGGER.infof("Creating category %s", category.getName() + ":" + category.getParent());
        emitter.send(KafkaRecord.of(null, category));
        return Response.accepted().build();
    }

    @PUT
    @Path("/parent/{parent}/name/{name}")
    public Response updateCategory(String categoryRequest, String parent, String name) {
        Builder categoryBuilder = Category.newBuilder();
        convertJSONToProtobuf(categoryRequest, categoryBuilder);
        Category category = categoryBuilder
            .setParent(parent)
            .setName(name)
            .setStatus(io.dmcapps.proto.catalog.Category.Status.PENDING)
            .build();
        LOGGER.infof("Updating category %s", category.getName());
        emitter.send(KafkaRecord.of(category.getId(), category));
        return Response.accepted().build();
    }

    @DELETE
    @Path("/parent/{parent}/name/{name}")
    public Response delteCategory(@PathParam("parent") String parent, @PathParam("name") String name) {
        LOGGER.infof("Deleting category %s", name + ":" + parent);
        emitter.send(KafkaRecord.of(name + ":" + parent, null));
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