package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static io.vertx.starter.HttpServerVerticle.ACTION;
import static io.vertx.starter.HttpServerVerticle.CONFIG_WIKIDB_QUEUE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("Duplicates")
public class DatabaseVerticle extends AbstractVerticle {
    Logger log = LoggerFactory.getLogger(DatabaseVerticle.class);

    private final String CONFIG_WIKIDB_SQL_QUERIES_RES_FILE = "wikidb.sql-queries";
    private final String CONFIG_WIKIDB_SQL_QUERIES_RES_FILE_DEFAULT = "/db-queries.properties";

    private JDBCClient dbClient;

    private Map<SqlQueries, String> sqls = new EnumMap<>(SqlQueries.class);

    private String wikiDbQueue;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        loadSql()
            .compose(v -> prepareDb())
            .compose(v -> setupHandlers())
            .setHandler(startFuture.completer());
    }

    private Future<Void> loadSql() throws IOException {
        Future<Void> future = Future.future();
        String resourcePath = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RES_FILE);
        try (InputStream is = resourcePath == null?
            getClass().getResourceAsStream(CONFIG_WIKIDB_SQL_QUERIES_RES_FILE_DEFAULT):
            new FileInputStream(resourcePath)) {

            Properties sqlProps = new Properties();
            sqlProps.load(is);

            sqls.put(SqlQueries.SQL_CREATE_PAGES_TABLE, sqlProps.getProperty("create-pages-table"));
            sqls.put(SqlQueries.SQL_CREATE_PAGE, sqlProps.getProperty("create-page"));
            sqls.put(SqlQueries.SQL_ALL_PAGES, sqlProps.getProperty("all-pages"));
            sqls.put(SqlQueries.SQL_GET_PAGE, sqlProps.getProperty("get-page"));
            sqls.put(SqlQueries.SQL_DELETE_PAGE, sqlProps.getProperty("delete-page"));
            sqls.put(SqlQueries.SQL_SAVE_PAGE, sqlProps.getProperty("save-page"));
            future.complete();
        } catch (IOException ioe) {
            future.fail(ioe);
        }
        return future;
    }


    private Future<Void> prepareDb() {
        Future<Void> future = Future.future();
        wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, CONFIG_WIKIDB_QUEUE);

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
                sqlConnection.execute(sqls.get(SqlQueries.SQL_CREATE_PAGES_TABLE), sqlRes -> {
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


    private Future<Void> setupHandlers() {
        Future<Void> future = Future.future();
        vertx.eventBus().<JsonObject>consumer(wikiDbQueue)
            .handler(this::handleRequest)
            .completionHandler(future.completer());
        return future;
    }

    private void handleRequest(Message<JsonObject> reqData) {
        if (reqData.headers().contains(ACTION)) {
            log.error("No action in message (headers: {}, body: {}). Don't know what to do", reqData.headers(),
                reqData.body().encodePrettily());
            reqData.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header provided");
            return;
        }
        String action = reqData.headers().get(ACTION);

        switch (action) {
            case "get-page":
                this.queryPage(reqData);
                break;
            case "all-pages":
                this.indexHandler(reqData);
                break;
            case "create-page":
                this.createPage(reqData);
                break;
            case "save-page":
                this.updatePage(reqData);
                break;
            case "delete-page":
                this.pageDeletionHandler(reqData);
                break;
            default:
                reqData.fail(ErrorCodes.BAD_ACTION.ordinal(), "Invalid action '" + action + "'. No handlers found");
        }
    }


    private void pageDeletionHandler(Message<JsonObject> ctxt) {
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

    private void createPage(Message<JsonObject> ctxt) {
        String pageName = ctxt.request().getParam("name");
        redirect(ctxt, pageName);
    }

    private void updatePage(Message<JsonObject> ctxt) {
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

    private void queryPage(Message<JsonObject> msg) {
        String pageName = msg.body().getString("page");
        dbClient.queryWithParams(sqls.get(SqlQueries.SQL_GET_PAGE), new JsonArray(singletonList(pageName)), fetchResult -> {
            if (fetchResult.failed()) {
                reportQueryError(msg, fetchResult.cause());
            } else {
                JsonObject resp = new JsonObject();
                ResultSet resSet = fetchResult.result();
                if (resSet.getNumRows() == 0) {
                    resp.put("found", false);
                } else {
                    resp.put("found", true);
                    JsonArray row = resSet.getResults().get(0);
                    resp.put("id", row.getInteger(0));
                    resp.put("rawContent", row.getString(1));
                }
                msg.reply(resp);
            }
        });
    }

    private void reportQueryError(Message<JsonObject> message, Throwable cause) {
        log.error("Database query error", cause);
        message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
    }

    private void indexHandler(Message<JsonObject> routingContext) {
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

    enum ErrorCodes {
        NO_ACTION_SPECIFIED,
        BAD_ACTION,
        DB_ERROR
    }

    enum SqlQueries {
        SQL_CREATE_PAGES_TABLE,
        SQL_GET_PAGE,
        SQL_CREATE_PAGE,
        SQL_SAVE_PAGE,
        SQL_ALL_PAGES,
        SQL_DELETE_PAGE
    }
}
