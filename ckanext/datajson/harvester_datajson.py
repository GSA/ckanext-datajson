from ckanext.datajson.harvester_base import DatasetHarvesterBase
from parse_datajson import parse_datajson_entry


import urllib2, json

class DataJsonHarvester(DatasetHarvesterBase):
    '''
    A Harvester for /data.json files.
    '''

    HARVESTER_VERSION = "0.9al"  # increment to force an update even if nothing has changed

    def info(self):
        return {
            'name': 'datajson',
            'title': '/data.json',
            'description': 'Harvests remote /data.json files',
        }

    def load_remote_catalog(self, harvest_job):
        req = urllib2.Request(harvest_job.source.url)
        # todo: into config and across harvester
        req.add_header('User-agent', 'Data.gov/2.0')
        try:
            datasets = json.load(urllib2.urlopen(req))
        except UnicodeDecodeError:
            # try different encode
            try:
                datasets = json.load(urllib2.urlopen(req), 'cp1252')
            except:
                datasets = json.load(urllib2.urlopen(req), 'iso-8859-1')
        except:
            # remove BOM
            datasets = json.loads(lstrip_bom(urllib2.urlopen(req).read()))

        # The first dataset should be for the data.json file itself. Check that
        # it is, and if so rewrite the dataset's title because Socrata exports
        # these items all with the same generic name that is confusing when
        # harvesting a bunch from different sources. It should have an accessURL
        # but Socrata fills the URL of these in under webService.
        if isinstance(datasets, list) and len(datasets) > 0 and (datasets[0].get("accessURL") == harvest_job.source.url
            or datasets[0].get("webService") == harvest_job.source.url) and \
            datasets[0].get("title") == "Project Open Data, /data.json file":
            datasets[0]["title"] = "%s Project Open Data data.json File" % harvest_job.source.title

        catalog_values = None
        if isinstance(datasets, dict):
            # this is a catalog, not dataset array as in schema 1.0.
            catalog_values = datasets.copy()
            datasets = catalog_values.pop("dataset", [])

            # hotfix to deal with an unexpected domain redirection
            for k, v in catalog_values.items():
                catalog_values[k] = v.replace("https://project-open-data.cio.gov/v1.1/schema", "https://resources.data.gov/schemas/dcat-us/v1.1/schema")
    
        for dataset in datasets:
            if dataset.get("license") == "https://project-open-data.cio.gov/unknown-license":
                dataset["license"] = "https://github.com/project-open-data/project-open-data.github.io/blob/master/unknown-license.md"
            elif dataset.get("license") == "https://project-open-data.cio.gov/open-licenses":
                dataset["license"] = "https://resources.data.gov/open-licenses/"
        
        return (datasets, catalog_values)
        
    def set_dataset_info(self, pkg, dataset, dataset_defaults, schema_version):
        parse_datajson_entry(dataset, pkg, dataset_defaults, schema_version)

# helper function to remove BOM
def lstrip_bom(str_):
    from codecs import BOM_UTF8
    bom = BOM_UTF8
    if str_.startswith(bom):
        return str_[len(bom):]
    else:
        return str_
