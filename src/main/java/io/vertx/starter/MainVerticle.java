package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.ext.web.templ.TemplateEngine;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class MainVerticle extends AbstractVerticle {
    Logger log = LoggerFactory.getLogger(MainVerticle.class);

    private JDBCClient dbClient;

    private TemplateEngine freemarker = FreeMarkerTemplateEngine.create();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        prepareDb().compose(v -> startServer()).setHandler(startFuture.completer());
    }

    private Future<Void> prepareDb() {
        Future<Void> future = Future.future();
        dbClient = JDBCClient.createShared(vertx, new JsonObject()
            .put("url", "jdbc:hsqldb:file:db/wiki")
            .put("driver_class", "org.hsqldb.jdbcDriver")
            .put("max_pool_size", 30));

        dbClient.getConnection(res -> {
            if (res.failed()) {
                log.error("Cannot connect to database", res.cause());
                future.fail(res.cause());
            } else {
                SQLConnection sqlConnection = res.result();
                sqlConnection.execute(SQL_CREATE_PAGES_TABLE, sqlRes -> {
                    if (sqlRes.failed()) {
                        log.error("Cannot create table in db", sqlRes.cause());
                        future.fail(sqlRes.cause());
                    } else {
                        future.complete();
                    }
                });
            }
        });
        return future;
    }

    private Future<Void> startServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/wiki/:page").handler(this::pageRenderingHandler);
        router.post().handler(BodyHandler.create());
        router.post("/save").handler(this::pageUpdateHandler);
        router.post("/create").handler(this::pageCreateHandler);
        router.post("/delete").handler(this::pageDeletionHandler);

        server.requestHandler(router::accept)
            .listen(8080, res -> {
                if (res.succeeded()) {
                    log.info("Start http server");
                    future.complete();
                } else {
                    log.error("Cant start http server");
                    future.fail(res.cause());
                }
            });

        return future;
    }

    private void pageDeletionHandler(RoutingContext routingContext) {
    }

    private void pageCreateHandler(RoutingContext routingContext) {

    }

    private void pageUpdateHandler(RoutingContext routingContext) {

    }

    private void pageRenderingHandler(RoutingContext routingContext) {

    }

    private void indexHandler(RoutingContext routingContext) {
        dbClient.getConnection(ar -> {
            if (ar.failed()) {
                routingContext.fail(ar.cause());
            } else {
                SQLConnection sqlConnection = ar.result();
                sqlConnection.query(SQL_ALL_PAGES, sqlRes -> {
                    sqlConnection.close();
                    if (sqlRes.failed()) {
                        routingContext.fail(sqlRes.cause());
                    } else {
                        List<String> pageNames = sqlRes.result().getResults().stream()
                            .map(jsonArr -> jsonArr.getString(0))
                            .sorted()
                            .collect(toList());

                        routingContext.put("title", "Wiki home")
                            .put("pages", pageNames);
                        freemarker.render(routingContext, "templates", "/index.ftl", buffRes -> {
                            if (buffRes.failed()) {
                                routingContext.fail(buffRes.cause());
                            } else {
                                routingContext.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/html")
                                    .end(buffRes.result());
                            }
                        });
                    }
                });
            }
        });
    }

    private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
    private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
    private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
    private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
    private static final String SQL_ALL_PAGES = "select Name from Pages";
    private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

}
