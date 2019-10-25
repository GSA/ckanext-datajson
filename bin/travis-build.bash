#!/bin/bash
set -e

echo "This is travis-build.bash..."

echo "Installing the packages that CKAN requires..."
sudo apt-get update -qq
sudo apt-get install solr-jetty libcommons-fileupload-java:amd64=1.2.2-1

echo "Installing CKAN and its Python dependencies..."
git clone https://github.com/gsa/ckan
cd ckan
if [ $CKANVERSION == '2.8' ]
then
	git checkout datagov
elif [ $CKANVERSION == '2.3' ]
then
	git checkout 2.3-upgrade
fi
python setup.py develop
cp ./ckan/public/base/css/main.css ./ckan/public/base/css/main.debug.css
pip install -r requirements.txt --allow-all-external
pip install -r dev-requirements.txt --allow-all-external

cd -

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'
sudo -u postgres psql -c 'CREATE DATABASE datastore_test WITH OWNER ckan_default;'

echo "Initialising the database..."
cd ckan
paster db init -c test-core.ini
cd -

cd ..
echo "Installing Harverter"
git clone https://github.com/gsa/ckanext-harvest
cd ckanext-harvest
if [ $CKANVERSION == 'release-datagov' ]
then
	git checkout inventory
fi

python setup.py develop
pip install -r pip-requirements.txt --allow-all-external
cd -

echo "Installing ckanext-datajson and its requirements..."
cd ckanext-datajson
pip install -r pip-requirements.txt --allow-all-external
python setup.py develop


echo "Moving test.ini into a subdir..."
mkdir subdir
mv test.ini subdir

echo "travis-build.bash is done."