ckanext-datajson
================

[![Build Status](https://travis-ci.org/GSA/ckanext-datajson.svg?branch=master)](https://travis-ci.org/GSA/ckanext-datajson)
[![Codacy Badge](https://api.codacy.com/project/badge/c4816041270448b6a0c9760933065b7e)](https://www.codacy.com/app/alexandr-perfilov/ckanext-datajson)
[![Coverage Status](https://coveralls.io/repos/GSA/ckanext-datajson/badge.svg?branch=master&service=github)](https://coveralls.io/github/GSA/ckanext-datajson?branch=master)

A CKAN extension containinig plugins ``datajson``.
First is used by http://catalog.data.gov/ to harvest data sources 
from a remote /data.json file according to the U.S. Project
Open Data metadata specification (https://project-open-data.cio.gov/).

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

Providing OMB Bureau Codes
--------------------------

The ```datajsonvalidator.py``` file needs a list of OMB agency 
and bureau codes. With no configuration, this information is
automatically pulled from the Project Open Data web site. However,
you can provide a local file with the same information using the
```CKAN_BUREAU_CSV_FILE``` environment variable. Alternatively,
you can use the ```CKAN_BUREAU_CSV_URL``` environment variable to
specify a different URL to fetch the information.

By default, the URL is https://project-open-data.cio.gov/data/omb_bureau_codes.csv.

An example csv file is provided named ```test_omb_bureau_codes.csv```. This
file can be used by running the following command before starting
this extension

```
export CKAN_BUREAU_CSV_FILE=test_omb_bureau_codes.csv
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
