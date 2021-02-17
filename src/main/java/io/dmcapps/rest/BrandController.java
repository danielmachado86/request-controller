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

import io.dmcapps.proto.catalog.Brand;
import io.dmcapps.proto.catalog.Brand.Builder;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@Path("/brands")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BrandController {

    private static final Logger LOGGER = Logger.getLogger("RequestController");

    @Inject
    @Channel("brands")
    Emitter<Brand> emitter;

    @POST
    public Response addBrand(String brandRequest) {
        Builder brandBuilder = Brand.newBuilder();
        convertJSONToProtobuf(brandRequest, brandBuilder);
        brandBuilder.setStatus(io.dmcapps.proto.catalog.Brand.Status.PENDING);
        Brand brand = brandBuilder.build();
        if (brand.getName().isEmpty()) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        LOGGER.infof("Creating brand %s", brand.getName());
        emitter.send(KafkaRecord.of(null, brand));
        return Response.accepted().build();
    }

    @PUT
    @Path("/{name}")
    public Response updateBrand(String brandRequest, String name) {
        Builder brandBuilder = Brand.newBuilder();
        convertJSONToProtobuf(brandRequest, brandBuilder);
        Brand brand = brandBuilder
            .setName(name)
            .setStatus(io.dmcapps.proto.catalog.Brand.Status.PENDING)
            .build();
        LOGGER.infof("Updating brand %s", brand.getName());
        emitter.send(KafkaRecord.of(brand.getId(), brand));
        return Response.accepted().build();
    }

    @DELETE
    @Path("/{name}")
    public Response delteBrand(@PathParam("name") String name) {
        LOGGER.infof("Deleting brand %s", name);
        emitter.send(KafkaRecord.of(name, null));
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