ARG CKAN_VERSION=2.11
FROM ckan/ckan-dev:${CKAN_VERSION}

USER root

COPY . $APP_DIR/

# python cryptography takes a while to build
RUN pip install -r $APP_DIR/requirements.txt -r $APP_DIR/dev-requirements.txt -e $APP_DIR/.
