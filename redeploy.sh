#!/usr/bin/env bash

export LAUNCHER="io.vertx.core.Launcher"
export VERTICLE="io.vertx.starter.MainVerticle"
export CMD="mvn compile"
export VERTX_CMD="run"

#mvn compile dependency:copy-dependencies
java \
  -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
  -Dlogback.=io.vertx.core.logging.SLF4JLogDelegateFactory \
  -jar  ./target/vertx-wiki-1.0-SNAPSHOT-fat.jar \
  $LAUNCHER $VERTX_CMD $VERTICLE


  -cp  $(echo target/dependency/*.jar | tr ' ' ':'):"target/classes" \
  $LAUNCHER $VERTX_CMD $VERTICLE \
  --redeploy="src/main/**/*" --on-redeploy="$CMD" \
  --launcher-class=$LAUNCHER \
  $@
