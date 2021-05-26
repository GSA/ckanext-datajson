#!/bin/bash
# TODO delete this after we are on CKAN 2.9 in Catalog and Inventory.
set -e

echo "Updating script ..."

# Copy the shared CI script.
wget https://raw.githubusercontent.com/GSA/catalog.data.gov/fcs/tools/ci-scripts/circleci-build-catalog-next.bash
wget https://raw.githubusercontent.com/GSA/catalog.data.gov/fcs/ckan/test-catalog-next.ini

sudo chmod +x circleci-build-catalog-next.bash
source circleci-build-catalog-next.bash

echo "Update ckanext-datajson"
python setup.py develop

echo "TESTING ckanext-datajson"
nosetests --ckan --with-pylons=test-catalog-next.ini ckanext/datajson/tests/nose --debug=ckanext
