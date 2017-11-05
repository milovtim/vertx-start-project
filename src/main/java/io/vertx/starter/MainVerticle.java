package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.ext.web.templ.TemplateEngine;

import java.util.Date;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.SEE_OTHER;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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

    private void pageDeletionHandler(RoutingContext ctxt) {
        String pageId = ctxt.request().getParam("id");

        dbClient.getConnection(connRes -> {
            if (connRes.failed()) {
                ctxt.fail(connRes.cause());
            } else {
                connRes.result().updateWithParams(SQL_DELETE_PAGE, new JsonArray(singletonList(pageId)), sqlRes -> {
                    int updated = sqlRes.result().getUpdated();
                    connRes.result().close();
                    if (updated == 1) {
                        log.debug("Page with id={} was deleted", pageId);
                    } else {
                        log.warn("Cant delete page. No page with id={} found", pageId);
                    }
                    redirect(ctxt, null);
                });
            }
        });
    }

    private void pageCreateHandler(RoutingContext ctxt) {
        String pageName = ctxt.request().getParam("name");
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
        HttpServerRequest req = ctxt.request();
        String pageId = req.getParam("id");
        String title = req.getParam("title");
        String markdown = req.getParam("markdown");
        Boolean newPage = Boolean.valueOf(req.getParam("newPage"));

        dbClient.getConnection(connRes -> {
            if (connRes.failed()) {
                ctxt.fail(connRes.cause());
            } else {
                SQLConnection conn = connRes.result();
                String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
                JsonArray params = newPage ? new JsonArray(asList(title, markdown)) : new JsonArray(asList(markdown, pageId));
                conn.updateWithParams(sql, params, sqlRes -> {
                    conn.close();
                    if (sqlRes.failed()) {
                        ctxt.fail(sqlRes.cause());
                    } else {
                        log.debug("{} page named '{}'", newPage? "Create": "Update", title);
                        redirect(ctxt, title);
                    }
                });
            }
        });
    }

    private static final String EMPTY_PAGE_TMPL =
        "This is the new empty page\n\nUse markdown syntax to write you text";

    private void pageRenderingHandler(RoutingContext rCtxt) {
        Object pageName = rCtxt.request().getParam("page");

        dbClient.getConnection(connRes -> {
            if (connRes.failed()) {
                rCtxt.fail(connRes.cause());
            } else {
                SQLConnection sqlConnection = connRes.result();
                sqlConnection.queryWithParams(SQL_GET_PAGE, new JsonArray(singletonList(pageName)), resultSetRes -> {
                    if (resultSetRes.failed()) {
                        rCtxt.fail(resultSetRes.cause());
                    } else {
                        ResultSet resSet = resultSetRes.result();
                        final boolean[] isNewPage = {false};
                        JsonArray pageIdAndContent = resSet.getResults().stream()
                            .findFirst()
                            .orElseGet(() -> {
                                isNewPage[0] = true; //if no rows in resultSet, set 'newPage' flag
                                return new JsonArray(asList(-1, EMPTY_PAGE_TMPL));
                            });
                        Integer pageId = pageIdAndContent.getInteger(0);
                        String pageContent = pageIdAndContent.getString(1);

                        rCtxt.put("title", pageName)
                            .put("id", pageId)
                            .put("newPage", isNewPage[0])
                            .put("rawContent", pageContent)
                            .put("content", Processor.process(pageContent))
                            .put("timestamp", new Date().toString());

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
        });
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
