ckanext-datajson
================

[![CircleCI](https://circleci.com/gh/GSA/ckanext-datajson.svg?style=svg)](https://circleci.com/gh/GSA/ckanext-datajson)

A CKAN extension containinig plugins ``datajson``.
First is used by http://catalog.data.gov/ to harvest data sources 
from a remote /data.json file according to the U.S. Project
Open Data metadata specification (https://resources.data.gov/schemas/dcat-us/v1.1/).

Plugin ``datajson`` provides a harvester to import datasets from other
remote /data.json files. See below for setup instructions.

And the plugin also provides a new view to validate /data.json files
at http://ckanhostname/pod/validate.

Installation
------------

To install, activate your CKAN virtualenv, install dependencies, and
install the module in develop mode, which just puts the directory in your
Python path.

	. path/to/pyenv/bin/activate
	pip install -r pip-requirements.txt
	python setup.py develop

Then in your CKAN .ini file, add ``datajson``
to your ckan.plugins line:

	ckan.plugins = (other plugins here...) datajson

That's the plugin for /data.json output. To make the harvester available,
also add:

	ckan.plugins = (other plugins here...) harvest datajson_harvest

If you're running CKAN via WSGI, we found a strange Python dependency
bug. It might only affect development environments. The fix was to
revise wsgi.py and add:

	import ckanext

before

	from paste.deploy import loadapp

Then restart your server and check out:

	http://yourdomain.com/data.json
	   and
	http://yourdomain.com/data.jsonld
	   and
	http://yourdomain.com/pod/validate	

## Using the Docker Dev Environment

### Build Environment

To start environment, run:
```docker-compose build```
```docker-compose up```

CKAN will start at localhost:5000

To shut down environment, run:

```docker-compose down```

To docker exec into the CKAN image, run:

```docker-compose exec ckan /bin/bash```

### Run Tests with Docker

```docker-compose exec ckan /bin/bash -c "nosetests --ckan --with-pylons=src/ckan/test-catalog-next.ini src_extensions/datajson/"```

### Run test with Classic catalog-app (CKAN 2.3)

Using the [catalog-app](https://github.com/GSA/catalog-app) repo it's possible to run tests

```
docker-compose -f docker-compose.yml -f docker-compose.local.yml exec app bash
	source /usr/lib/ckan/bin/activate
	cd /usr/lib/ckan/src/ckanext-datajson
	pip install -r dev-classic-catalog-requirements.txt
	nosetests --ckan \
		--with-pylons=test-classic-catalog-ckan-2.3.ini \
		ckanext.datajson.tests
```

Caching The Response
--------------------

If you're deploying inside Apache, some caching would be a good idea
because generating the /data.json file can take a good few moments.
Enable the cache modules:

	a2enmod cache
	a2enmod disk_cache

And then in your Apache configuration add:

	CacheEnable disk /data.json
	CacheRoot /tmp/apache_cache
	CacheDefaultExpire 120
	CacheMaxFileSize 50000000
	CacheIgnoreCacheControl On
	CacheIgnoreNoLastMod On
	CacheStoreNoStore On

And be sure to create /tmp/apache_cache and make it writable by the Apache process.

If you are deploying in a container environment, a new route has been included: cache_data.json.
This will create the data.json file and save it locally, which will then be served on subsequent
requests to /data.json. You can hit cache_data.json again to update. This cache_data.json request
should time out if the number of datasets is large (> 1000 or so).

Generating /data.json Off-Line
------------------------------

Generating this file is a little slow, so an alternative instead of caching is
to generate the file periodically (e.g. in a cron job). In that case, you'll want
to change the path that CKAN generates the file at to something *other* than /data.json.
In your CKAN .ini file, in the app:main section, add:

	ckanext.datajson.path = /internal/data.json

Now create a crontab file ("mycrontab") to download this URL to a file on disk
every ten minutes:

	0-59/10 * * * * wget -qO /path/to/static/data.json http://localhost/internal/data.json

And activate your crontab like so:

	crontab mycrontab

In Apache, we'll want to block outside access to the "internal" URL, and also
map the URL /data.json to the static file. In your httpd.conf, add:

	Alias /data.json /path/to/static/data.json
	
	<Location /internal/>
		Order deny,allow
		Allow from 127.0.0.1
		Deny from all
	</Location>

And then restart Apache. Wait for the cron job to run once, then check if
/data.json loads (and it should be fast!). Also double check that 
http://yourdomain.com/internal/data.json gives a 403 forbidden error when
accessed from some other location.

Options
-------

You can customize the URL that generates the data.json output:

	ckanext.datajson.path = /data.json
	ckanext.datajsonld.path = /data.jsonld
	ckanext.datajsonld.id = http://www.youragency.gov/data.json

You can enable or disable the Data.json output by setting

    ckanext.datajson.url_enabled = False

If ckanext.datajsonld.path is omitted, it defaults to replacing ".json" in your
ckanext.datajson.path path with ".jsonld", so it probably won't need to be
specified.

The option ckanext.datajsonld.id is the @id value used to identify the data
catalog itself. If not given, it defaults to ckan.site_url.

The Harvester
-------------

To use the data.json harvester, you'll also need to set up the CKAN harvester
extension. See the CKAN harvester README at https://github.com/okfn/ckanext-harvest
for how to do that. You'll set some configuration variables and then initialize the
CKAN harvester plugin using:

	paster --plugin=ckanext-harvest harvester initdb --config=/path/to/ckan.ini

Now you can set up a new DataJson harvester by visiting:

	http://yourdomain.com/harvest

And when configuring the data source, just choose "/data.json" as the source type.

**The next paragraph assumes you're using my fork of the CKAN harvest extension
at https://github.com/JoshData/ckanext-harvest**

In the configuration field, you can put a YAML string containing defaults for fields
that may not be set in the source data.json files, e.g. enter something like this:

	defaults:
	  Agency: Department of Health & Human Services
	  Author: Substance Abuse & Mental Health Services Administration
	  author_id: http://healthdata.gov/id/agency/samhsa

This again is tied to the HealthData.gov metadata schema.

Credit / Copying
----------------

Original work written by the HealthData.gov team. It has been modified in support of Data.gov.

As a work of the United States Government, this package is in the public 
domain within the United States. Additionally, we waive copyright and 
related rights in the work worldwide through the CC0 1.0 Universal 
public domain dedication (which can be found at http://creativecommons.org/publicdomain/zero/1.0/).

## Ways to Contribute
We're so glad you're thinking about contributing to ckanext-datajson!

Before contributing to ckanext-datajson we encourage you to read our [CONTRIBUTING](CONTRIBUTING.md) guide, our [LICENSE](LICENSE.md), and our README (you are here), all of which should be in this repository. If you have any questions, you can email the Data.gov team at [datagov@gsa.gov](mailto:datagov@gsa.gov).
