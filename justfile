build:
    #!/usr/bin/env bash
    JS_MODULES=$(nix build .#dagster-js-modules --print-out-paths --no-link)
    mkdir -p dagster_webserver/webapp
    if [ -L dagster_webserver/webapp/build ]; then
        rm dagster_webserver/webapp/build
    fi
    ln -s "$JS_MODULES/lib/dagster-app-oss" dagster_webserver/webapp/build

clean:
    rm -rf dagster_webserver/webapp
