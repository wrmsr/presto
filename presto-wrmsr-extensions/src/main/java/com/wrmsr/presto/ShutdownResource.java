package com.wrmsr.presto;

import com.google.common.collect.ImmutableMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/shutdown")
public class ShutdownResource
{
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(@Context HttpServletRequest servletRequest)
    {
        return Response.ok(ImmutableMap.of(), MediaType.APPLICATION_JSON_TYPE).build();
    }
}
