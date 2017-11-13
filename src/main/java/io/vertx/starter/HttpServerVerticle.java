package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.ext.web.templ.TemplateEngine;

import java.util.Date;

import static io.netty.handler.codec.http.HttpResponseStatus.SEE_OTHER;
import static java.lang.Boolean.valueOf;

public class HttpServerVerticle extends AbstractVerticle {
    Logger log = LoggerFactory.getLogger(HttpServerVerticle.class);

    public static final String ACTION = "action";
    public static final String CONFIG_HTTP_SERVER_PORT = "server.port";
    public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

    private String wikiDbQueue;


    private TemplateEngine freemarker = FreeMarkerTemplateEngine.create();

    @Override
    public void start(Future<Void> ftre) throws Exception {
        wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");
        Integer serverPort = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/wiki/:page").handler(this::pageRenderingHandler);
        router.post().handler(BodyHandler.create());
        router.post("/save").handler(this::pageUpdateHandler);
        router.post("/create").handler(this::pageCreateHandler);
        router.post("/delete").handler(this::pageDeletionHandler);

        vertx.createHttpServer().requestHandler(router::accept).listen(serverPort, res -> {
            if (res.succeeded()) {
                log.info("Start http server on port={}", serverPort);
                ftre.complete();
            } else {
                log.error("Cant start http server", res.cause());
                ftre.fail(res.cause());
            }
        });
    }


    private void pageDeletionHandler(RoutingContext ctxt) {
        String id = ctxt.request().getParam("id");
        log.debug("Handle page (id={}) deletion http method", id);
        JsonObject request = new JsonObject().put("id", id);
        DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, "delete-page");
        vertx.eventBus().send(wikiDbQueue, request, options, reply -> {
            if (reply.succeeded()) {
                ctxt.response().setStatusCode(303);
                ctxt.response().putHeader("Location", "/");
                ctxt.response().end();
            } else {
                ctxt.fail(reply.cause());
            }
        });
    }

    private void pageCreateHandler(RoutingContext ctxt) {
        String pageName = ctxt.request().getParam("name");
        log.debug("Handler page (name={}) creation http method", pageName);
        redirect(ctxt, pageName);
    }

    private void redirect(RoutingContext ctxt, String pageName) {
        String location = (pageName == null || pageName.isEmpty()) ? "/" : "/wiki/" + pageName;
        ctxt.response()
            .putHeader(HttpHeaders.LOCATION, location)
            .setStatusCode(SEE_OTHER.code())
            .end();
    }

    private void pageUpdateHandler(RoutingContext ctxt) {
        String title = ctxt.request().getParam("title");
        JsonObject request = new JsonObject()
            .put("id", ctxt.request().getParam("id"))
            .put("title", title)
            .put("markdown", ctxt.request().getParam("markdown"));

        log.debug("Handle page (title={}) update http method", title);
        DeliveryOptions options = new DeliveryOptions()
            .addHeader(ACTION, valueOf(ctxt.request().getParam("newPage")) ? "create-page" : "save-page");

        vertx.eventBus().send(wikiDbQueue, request, options, reply -> {
            if (reply.succeeded()) {
                redirect(ctxt, title);
            } else {
                ctxt.fail(reply.cause());
            }
        });
    }

    private static final String EMPTY_PAGE_TMPL =
        "This is the new empty page\n\nUse markdown syntax to write you text";

    private void pageRenderingHandler(RoutingContext rCtxt) {
        String pageName = rCtxt.request().getParam("page");

        JsonObject jsonRequest = new JsonObject().put("page", pageName);
        log.debug("Handle page (page={}) render http method", pageName);

        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader(ACTION, "get-page");

        vertx.eventBus().send(wikiDbQueue, jsonRequest, deliveryOptions, res -> {
            if (res.failed()) {
                rCtxt.fail(res.cause());
            } else {
                JsonObject body = (JsonObject) res.result().body();
                boolean found = body.getBoolean("found");
                String rawContent = body.getString("rawContent", EMPTY_PAGE_TMPL);

                context.put("title", pageName);
                context.put("id", body.getInteger("id", -1));
                context.put("newPage", Boolean.toString(found));
                context.put("rawContent", rawContent);
                context.put("content", Processor.process(rawContent));
                context.put("timestamp", new Date().toString());

                freemarker.render(rCtxt, "templates", "/page.ftl", renderRes -> {
                    if (renderRes.failed()) {
                        rCtxt.fail(renderRes.cause());
                    } else {
                        rCtxt.response()
                            .putHeader(HttpHeaders.CONTENT_TYPE, "text/html")
                            .end(renderRes.result());
                    }
                });
            }
        });
    }

    private void indexHandler(RoutingContext ctxt) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader(ACTION, "all-pages");

        log.debug("Handle index page http method");

        vertx.eventBus().send(wikiDbQueue, null, deliveryOptions, msgRes -> {
            if (msgRes.failed()) {
                ctxt.fail(msgRes.cause());
            } else {
                JsonObject body = ((JsonObject) msgRes.result().body());
                ctxt.put("title", "Wiki home")
                    .put("pages", body.getJsonArray("pages").getList());
                freemarker.render(ctxt, "templates", "/index.ftl", rendRes -> {
                    if (rendRes.failed()) {
                        ctxt.fail(rendRes.cause());
                    } else {
                        ctxt.response()
                            .putHeader(HttpHeaders.CONTENT_TYPE, "text/html")
                            .end(rendRes.result());
                    }
                });
            }
        });
    }
}
