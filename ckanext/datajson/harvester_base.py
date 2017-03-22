#!/usr/bin/env python
# -*- coding: utf-8 -*-


import hashlib
import json
import logging
import os
import uuid
import yaml
from ConfigParser import ConfigParser
from os import path, environ

import requests
from ckan import model
from ckan import plugins as p
from ckan.controllers.package import get_action
from ckan.lib.munge import munge_title_to_name
from ckan.lib.navl.dictization_functions import Invalid
from ckan.lib.navl.validators import ignore_empty
from ckan.lib.search.index import PackageSearchIndex
from ckan.logic import NotFound, get_action
from ckan.model import Session, Package
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from jsonschema import FormatChecker
from jsonschema.validators import Draft4Validator
from sqlalchemy.exc import IntegrityError

log = logging.getLogger("harvester")

VALIDATION_SCHEMA = [
    ('', 'Project Open Data (Federal)'),
    ('non-federal', 'Project Open Data (Non-Federal)'),
]


def validate_schema(schema):
    if schema not in [s[0] for s in VALIDATION_SCHEMA]:
        raise Invalid('Unknown validation schema: {0}'.format(schema))
    return schema


class DatasetHarvesterBase(HarvesterBase):
    """
    A Harvester for datasets.
    """
    _user_name = None

    def validate_config(self, config):
        if not config:
            return config
        config_obj = yaml.load(config)
        return config

    def load_config(self, harvest_source):
        # Load the harvest source's configuration data. We expect it to be a YAML
        # string. Unfortunately I went ahead of CKAN on this. The stock CKAN harvester
        # only allows JSON in the configuration box. My fork is necessary for this
        # to work: https://github.com/joshdata/ckanext-harvest

        ret = {
            "filters": {},  # map data.json field name to list of values one of which must be present
            "defaults": {},
        }

        source_config = yaml.load(harvest_source.config)

        try:
            ret["filters"].update(source_config["filters"])
        except TypeError:
            pass
        except KeyError:
            pass

        try:
            ret["defaults"].update(source_config["defaults"])
        except TypeError:
            pass
        except KeyError:
            pass

        return ret

    def _get_user_name(self):
        if not self._user_name:
            user = p.toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
            self._user_name = user['name']

        return self._user_name

    def context(self):
        return {"user": self._get_user_name(), "ignore_auth": True}

    # SUBCLASSES MUST IMPLEMENT
    def load_remote_catalog(self, harvest_job):
        raise Exception("Not implemented")

    def extra_schema(self):
        return {
            'validator_schema': [ignore_empty, unicode, validate_schema],
        }

    def gather_stage(self, harvest_job):
        log.debug('In %s gather_stage (%s)' % (repr(self), harvest_job.source.url))
        try:
            source_datasets, catalog_values = self.load_remote_catalog(harvest_job)
        except ValueError as e:
            self._save_gather_error("Error loading json content: %s." % (e), harvest_job)
            return []
        tmp_superThemes = ["agri", "educ", "econ", "ener",
                           "envi", "gove", "heal", "intr",
                           "just", "regi", "soci", "tech",
                           "tran"]
        ckan_host = ''
        # Call to config.ini to load superTheme list
        if 'CKAN_CONFIG' in environ:
            if path.exists(environ['CKAN_CONFIG']):
                try:
                    tmp_ckan_config = ConfigParser()
                    tmp_ckan_config.read(environ['CKAN_CONFIG'])
                except IOError:
                    log.warn('Error loading CKAN config.ini file [%s]. '
                             'Loading default SuperThemes', environ['CKAN_CONFIG'])
                except Exception:
                    log.warn('Unknow error loading CKAN config.ini file [%s]. '
                             'Loading default SuperThemes', environ['CKAN_CONFIG'])
                try:
                    ckan_host = tmp_ckan_config.get('app:main', 'ckan.site_url')
                except Exception:
                    log.warn('Error loading \"ckan.site_url\" from CKAN config.ini file [%s]. '
                             'Loading default SuperThemes', environ['CKAN_CONFIG'])
                # Get superThemeTaxonomy
                try:
                    if len(ckan_host) > 0:
                        stt_url = '{site_url}/superThemeTaxonomy.json'.format(site_url=ckan_host)
                        superThemeTaxonomy = requests.get(stt_url)
                        superThemeTaxonomy = superThemeTaxonomy.json()
                        if len(superThemeTaxonomy) < 0:
                            raise Exception('SuperThemeTaxonomy JSON in empty')
                        if 'id' not in [theme for theme in superThemeTaxonomy]:
                            raise Exception('SuperThemeTaxonomy JSON don\'t contains \"id\" field')
                        tmp_superThemes = [theme['id'] for theme in superThemeTaxonomy]
                        log.info("superThemeTaxonomy loaded!")
                    else:
                        raise Exception('The field of config.ini \"site_url\" is empty.')
                except Exception, e:
                    log.warn("Error getting \"ThemeTaxonomy.json\", err: %s.", e)
        superThemes = tmp_superThemes
        for dataset in source_datasets:
            # Delete if exists @type key
            try:
                del dataset['@type']
            except Exception:
                pass

            try:
                foo = dataset['theme']
                log.info('Theme exists and it value is:{0}.'.format(foo))
            except IndexError:
                log.warn('The field \"theme\" not exists!... Fill it MF!')
                dataset.update({'theme': []})
            try:
                tags = dataset['keyword']
                themes = dataset['theme']
                if len(themes) > 0:
                    if type(tags) is list:
                        dataset['keyword'] = tags + themes
                    else:
                        dataset['keyword'] = [tags] + themes
            except IndexError:
                pass
            try:
                dataset.update({'author_email': dataset['publisher']['mbox']})
            except IndexError:
                log.warn('El campo \"publisher\" para \"{0}\" no contine campo \"mbox\".'.format(dataset['title']))
                dataset.update({'author_mail': "unknow"})
            except Exception:
                log.warn('El fallo el campo \"publisher\" para \"{0}\". Este error es critico, '
                         'se completara el campo \"mbox\". para evitar errores futuros.'.format(dataset['title']))
                dataset.update({'author_email': "unknow"})

            try:
                dataset.update({'author': dataset['publisher']['name']})
            except IndexError:
                log.warn('El campo \"publisher\" para \"{0}\" no contine campo \"name\".'.format(dataset['title']))
                dataset.update({'author': "unknow"})
            except Exception:
                log.warn('El fallo el campo \"publisher\" para \"{0}\". Este error es critico, '
                         'se completara el campo \"name\". para evitar errores futuros.'.format(dataset['title']))
                dataset.update({'author': "unknow"})
            try:
                del dataset['publisher']
            except Exception:
                pass

            try:
                dataset.update({'maintainer_email': dataset['contactPoint']['hasEmail']})
                dataset.update({'maintainer': dataset['contactPoint']['fn']})
                del dataset['contactPoint']
            except Exception:
                dataset.update({'maintainer_email': ""})
                dataset.update({'maintainer': ""})
                del dataset['contactPoint']

        DATAJSON_SCHEMA = source_datasets
        schema_version = '1.2'
        parent_identifiers = set()
        child_identifiers = set()
        catalog_extras = {}
        if isinstance(catalog_values, dict):
            schema_version = '1.2'
            for dataset in source_datasets:
                parent_identifier = dataset.get('isPartOf')
                if parent_identifier:
                    parent_identifiers.add(parent_identifier)
                    child_identifiers.add(dataset.get('identifier'))

            # get a list of needed catalog values and put into hobj
            catalog_fields = ['title', 'description']
            catalog_extras = dict(('catalog_' + k, v)
                                  for (k, v) in catalog_values.iteritems()
                                  if k in catalog_fields)

        # Loop through the packages we've already imported from this source
        # and go into their extra fields to get their source_identifier,
        # which corresponds to the remote catalog's 'identifier' field.
        # Make a mapping so we know how to update existing records.
        # Added: mark all existing parent datasets.
        existing_datasets = {}
        existing_parents = {}
        for hobj in model.Session.query(HarvestObject).filter_by(source=harvest_job.source, current=True):
            try:
                pkg = get_action('package_show')(self.context(), {"id": hobj.package_id})
            except:
                # reference is broken
                continue
            sid = self.find_extra(pkg, "identifier")
            is_parent = self.find_extra(pkg, "collection_metadata")
            if sid:
                existing_datasets[sid] = pkg
            if is_parent and pkg.get("state") == "active":
                existing_parents[sid] = pkg

        # which parent has been demoted to child level?
        existing_parents_demoted = set(
            identifier for identifier in existing_parents.keys() \
            if identifier not in parent_identifiers)

        # which dataset has been promoted to parent level?
        existing_datasets_promoted = set(
            identifier for identifier in existing_datasets.keys() \
            if identifier in parent_identifiers \
            and identifier not in existing_parents.keys())

        # if there is any new parents, we will have to harvest parents
        # first, mark the status in harvest_source config, which
        # triggers a children harvest_job after parents job is finished.
        source = harvest_job.source
        source_config = json.loads(source.config or '{}')
        # run status: None, or parents_run, or children_run?
        run_status = source_config.get('datajson_collection')
        if parent_identifiers:
            for parent in parent_identifiers & child_identifiers:
                self._save_gather_error("Collection identifier '%s' \
                    cannot be isPartOf another collection." \
                                        % parent, harvest_job)

            new_parents = set(identifier for identifier in parent_identifiers \
                              if identifier not in existing_parents.keys())
            if new_parents:
                if not run_status:
                    # fresh start
                    run_status = 'parents_run'
                    source_config['datajson_collection'] = run_status
                    source.config = json.dumps(source_config)
                    source.save()
                elif run_status == 'children_run':
                    # it means new parents are tried and failed.
                    # but skip some which have previously reported with
                    # parent_identifiers & child_identifiers
                    for parent in new_parents - \
                            (parent_identifiers & child_identifiers):
                        self._save_gather_error("Collection identifier '%s' \
                            not found. Records which are part of this \
                            collection will not be harvested." \
                                                % parent, harvest_job)
                else:
                    # run_status was parents_run, and did not finish.
                    # something wrong but not sure what happened.
                    # let's leave it as it is, let it run one more time.
                    pass
            else:
                # all parents are already in place. run it as usual.
                run_status = None
        elif run_status:
            # need to clear run_status
            run_status = None
            source_config['datajson_collection'] = run_status
            source.config = json.dumps(source_config)
            source.save()

        # Create HarvestObjects for any records in the remote catalog.

        object_ids = []
        seen_datasets = set()
        unique_datasets = set()

        filters = self.load_config(harvest_job.source)["filters"]

        for dataset in source_datasets:
            # Create a new HarvestObject for this dataset and save the
            # dataset metdata inside it for later.

            # Check the config's filters to see if we should import this dataset.
            # For each filter, check that the value specified in the data.json file
            # is among the permitted values in the filter specification.
            matched_filters = True
            for k, v in filters.items():
                if dataset.get(k) not in v:
                    matched_filters = False
            if not matched_filters:
                continue

            if parent_identifiers and new_parents \
                    and dataset['identifier'] not in parent_identifiers \
                    and dataset.get('isPartOf') in new_parents:
                if run_status == 'parents_run':
                    # skip those whose parents still need to run.
                    continue
                else:
                    # which is 'children_run'.
                    # error out since parents got issues.
                    self._save_gather_error(
                        "Record with identifier '%s': isPartOf '%s' points to \
                        an erroneous record." % (dataset['identifier'],
                                                 dataset.get('isPartOf')), harvest_job)
                    continue

            # Some source contains duplicate identifiers. skip all except the first one
            if dataset['identifier'] in unique_datasets:
                self._save_gather_error("Duplicate entry ignored for identifier: '%s'." % (dataset['identifier']),
                                        harvest_job)
                continue
            unique_datasets.add(dataset['identifier'])

            # Get the package_id of this resource if we've already imported
            # it into our system. Otherwise, assign a brand new GUID to the
            # HarvestObject. I'm not sure what the point is of that.

            if dataset['identifier'] in existing_datasets:
                pkg = existing_datasets[dataset["identifier"]]
                pkg_id = pkg["id"]
                seen_datasets.add(dataset['identifier'])

                # We store a hash of the dict associated with this dataset
                # in the package so we can avoid updating datasets that
                # don't look like they've changed.
                if pkg.get("state") == "active" \
                        and dataset['identifier'] not in existing_parents_demoted \
                        and dataset['identifier'] not in existing_datasets_promoted \
                        and self.find_extra(pkg, "source_hash") == self.make_upstream_content_hash(dataset,
                                                                                                   harvest_job.source,
                                                                                                   catalog_extras,
                                                                                                   schema_version):
                    continue
            else:
                pkg_id = uuid.uuid4().hex

            # Create a new HarvestObject and store in it the GUID of the
            # existing dataset (if it exists here already) and the dataset's
            # metadata from the remote catalog file.
            extras = [HarvestObjectExtra(
                key='schema_version', value=schema_version)]
            if dataset['identifier'] in parent_identifiers:
                extras.append(HarvestObjectExtra(
                    key='is_collection', value=True))
            elif dataset.get('isPartOf'):
                parent_pkg_id = existing_parents[dataset.get('isPartOf')]['id']
                extras.append(HarvestObjectExtra(
                    key='collection_pkg_id', value=parent_pkg_id))
            # FIX EXTRAS
            # for k,v in

            for k, v in catalog_extras.iteritems():
                extras.append(HarvestObjectExtra(key=k, value=v))
            # ----
            obj = HarvestObject(
                guid=pkg_id,
                job=harvest_job,
                extras=extras,
                content=json.dumps(dataset,
                                   sort_keys=True))  # use sort_keys to preserve field order so hashes of this string are constant from run to run
            obj.save()
            object_ids.append(obj.id)

        # Remove packages no longer in the remote catalog.
        for upstreamid, pkg in existing_datasets.items():
            if upstreamid in seen_datasets: continue  # was just updated
            if pkg.get("state") == "deleted": continue  # already deleted
            pkg["state"] = "deleted"
            log.warn('deleting package %s (%s) because it is no longer in %s' % (
                pkg["name"], pkg["id"], harvest_job.source.url))
            get_action('package_update')(self.context(), pkg)
            obj = HarvestObject(
                guid=pkg_id,
                package_id=pkg["id"],
                job=harvest_job,
            )
            obj.save()
            object_ids.append(obj.id)

        return object_ids

    def fetch_stage(self, harvest_object):
        # Nothing to do in this stage because we captured complete
        # dataset metadata from the first request to the remote catalog file.
        return True

    # SUBCLASSES MUST IMPLEMENT
    def set_dataset_info(self, pkg, dataset, dataset_defaults, schema_version):
        # Sets package metadata on 'pkg' using the remote catalog's metadata
        # in 'dataset' and default values as configured in 'dataset_defaults'.
        raise Exception("Not implemented.")

    # validate dataset against POD schema
    # use a local copy.
    def _validate_dataset(self, validator_schema, schema_version, dataset):
        if validator_schema == 'non-federal':
            if schema_version == '1.1':
                file_path = 'pod_schema/non-federal-v1.1/dataset-non-federal.json'
            else:
                file_path = 'pod_schema/non-federal/single_entry.json'
        else:
            if schema_version == '1.1':
                file_path = 'pod_schema/federal-v1.1/dataset.json'
            else:
                file_path = 'pod_schema/single_entry.json'

        with open(os.path.join(
                os.path.dirname(__file__), file_path)) as json_file:
            schema = json.load(json_file)

        msg = ";"
        errors = Draft4Validator(schema, format_checker=FormatChecker()).iter_errors(dataset)
        count = 0
        for error in errors:
            count += 1
            msg = msg + " ### ERROR #" + str(count) + ": " + self._validate_readable_msg(error) + "; "
        msg = msg.strip("; ")
        if msg:
            id = "Identifier: " + (dataset.get("identifier") if dataset.get("identifier") else "Unknown")
            title = "Title: " + (dataset.get("title") if dataset.get("title") else "Unknown")
            msg = id + "; " + title + "; " + str(count) + " Error(s) Found. " + msg + "."
        return msg

    # make ValidationError readable.
    def _validate_readable_msg(self, e):
        msg = e.message.replace("u'", "'")
        elem = ""
        try:
            if e.schema_path[0] == 'properties':
                elem = e.schema_path[1]
                elem = "'" + elem + "':"
        except:
            pass

        return elem + msg

    def import_stage(self, harvest_object):
        # The import stage actually creates the dataset.

        log.debug('In %s import_stage' % repr(self))

        if (harvest_object.content == None):
            return True

        dataset = json.loads(harvest_object.content)
        schema_version = '1.2'  # default to '1.0'
        is_collection = False
        parent_pkg_id = ''
        catalog_extras = {}
        for extra in harvest_object.extras:
            if extra.key == 'schema_version':
                schema_version = extra.value
            if extra.key == 'is_collection' and extra.value:
                is_collection = True
            if extra.key == 'collection_pkg_id' and extra.value:
                parent_pkg_id = extra.value
            if extra.key.startswith('catalog_'):
                catalog_extras[extra.key] = extra.value

        # if this dataset is part of collection, we need to check if
        # parent dataset exist or not. we dont support any hierarchy
        # in this, so the check does not apply to those of is_collection
        if parent_pkg_id and not is_collection:
            parent_pkg = None
            try:
                parent_pkg = get_action('package_show')(self.context(),
                                                        {"id": parent_pkg_id})
            except:
                pass
            if not parent_pkg:
                parent_check_message = "isPartOf identifer '%s' not found." \
                                       % dataset.get('isPartOf')
                self._save_object_error(parent_check_message, harvest_object,
                                        'Import')
                return None

        # Get default values.
        dataset_defaults = self.load_config(harvest_object.source)["defaults"]

        source_config = json.loads(harvest_object.source.config or '{}')
        validator_schema = source_config.get('validator_schema')
        if schema_version == '1.2' and validator_schema != 'non-federal':
            lowercase_conversion = True
        else:
            lowercase_conversion = False

        MAPPING = {
            "title": "title",
            "identifier": "extras__identifier",
            "description": "notes",
            "keyword": "tags",
            "modified": "modified",
            "author": "author",
            "author_email": "author_email",
            "maintainer": "maintainer",
            "maintainer_email": "maintainer_email",
            "dataQuality": "extras__dataQuality",
            "license": "license_title",
            "spatial": "extras__spatial",
            "temporal": "extras__temporal",
            "superTheme": "groups",
            "primaryITInvestmentUII": "extras__primaryITInvestmentUII",
            "accrualPeriodicity": "extras__updateFrequency",
            "landingPage": "url",
            "language": "extras__language",
            "references": "extras__references",
            "issued": "extras__issued",
            "distribution": None,
        }

        MAPPING_V1_1 = {
            "title": "title",
            "identifier": "extras__identifier",
            "description": "notes",
            "keyword": "tags",
            "modified": "modified",
            "author": "author",
            "author_email": "author_email",
            "maintainer": "maintainer",
            "maintainer_email": "maintainer_email",
            "dataQuality": "extras__dataQuality",
            "license": "license_title",
            "spatial": "extras__spatial",
            "temporal": "extras__temporal",
            "superTheme": "groups",
            "primaryITInvestmentUII": "extras__primaryITInvestmentUII",
            "accrualPeriodicity": "extras__updateFrequency",
            "landingPage": "url",
            "language": "extras__language",
            "references": "extras__references",
            "issued": "extras__issued",
            "distribution": None,
        }
        MAPPING_V1_2 = {
            "title": "title",
            "identifier": "extras__identifier",
            "description": "notes",
            "keyword": "tags",
            "modified": "modified",
            "author": "author",
            "author_email": "author_email",
            "maintainer": "maintainer",
            "maintainer_email": "maintainer_email",
            "dataQuality": "extras__dataQuality",
            "license": "license_title",
            "spatial": "extras__spatial",
            "temporal": "extras__temporal",
            "superTheme": "groups",
            "primaryITInvestmentUII": "extras__primaryITInvestmentUII",
            "accrualPeriodicity": "extras__updateFrequency",
            "landingPage": "url",
            "language": "extras__language",
            "references": "extras__references",
            "issued": "extras__issued",
            "distribution": None,
        }

        SKIP = ["accessURL", "webService", "format", "distribution", "processed_how"]

        SKIP_V1_1 = ["@type", "isPartOf", "license", "distribution", "processed_how"]

        if lowercase_conversion:

            mapping_processed = {}
            for k, v in MAPPING.items():
                mapping_processed[k.lower()] = v

            skip_processed = [k.lower() for k in SKIP]

            dataset_processed = {'processed_how': ['lowercase']}
            for k, v in dataset.items():
                if k.lower() in mapping_processed.keys():
                    dataset_processed[k.lower()] = v
                else:
                    dataset_processed[k] = v

            if 'distribution' in dataset and dataset['distribution'] is not None:
                dataset_processed['distribution'] = []
                for d in dataset['distribution']:
                    d_lower = {}
                    for k, v in d.items():
                        if k.lower() in mapping_processed.keys():
                            d_lower[k.lower()] = v
                        else:
                            d_lower[k] = v
                    dataset_processed['distribution'].append(d_lower)
        else:
            dataset_processed = dataset
            mapping_processed = MAPPING
            skip_processed = SKIP

        if schema_version == '1.1':
            mapping_processed = MAPPING_V1_1
            skip_processed = SKIP_V1_1
        if schema_version == '1.2':
            mapping_processed = MAPPING_V1_2
            skip_processed = SKIP_V1_1

        validate_message = self._validate_dataset(validator_schema,
                                                  schema_version, dataset_processed)
        if validate_message:
            self._save_object_error(validate_message, harvest_object, 'Import')
            return None

        # We need to get the owner organization (if any) from the harvest
        # source dataset
        owner_org = None
        source_dataset = model.Package.get(harvest_object.source.id)
        if source_dataset.owner_org:
            owner_org = source_dataset.owner_org

        source_config = json.loads(harvest_object.source.config or '{}')
        # group_name = source_config.get('default_groups', '')
        group_name = [{'name': theme.lower()} for theme in dataset['superTheme']]

        # Assemble basic information about the dataset.

        pkg = {
            "state": "active",  # in case was previously deleted
            "owner_org": owner_org,
            "groups": group_name,
            "resources": [],
            "extras": [
                {
                    "key": "resource-type",
                    "value": "Dataset",
                },
                {
                    "key": "source_hash",
                    "value": self.make_upstream_content_hash(dataset, harvest_object.source, catalog_extras,
                                                             schema_version),
                },
                {
                    "key": "source_datajson_identifier",
                    "value": True,
                },
                {
                    "key": "harvest_source_id",
                    "value": harvest_object.harvest_source_id,
                },
                {
                    "key": "harvest_object_id",
                    "value": harvest_object.id,
                },
                {
                    "key": "harvest_source_title",
                    "value": harvest_object.source.title,
                },
                {
                    "key": "source_schema_version",
                    "value": schema_version,
                },
            ]
        }

        extras = pkg["extras"]
        unmapped = []

        for key, value in dataset_processed.iteritems():
            if key in skip_processed:
                continue
            new_key = mapping_processed.get(key)
            if not new_key:
                unmapped.append(key)
                continue

            # after schema 1.0+, we need to deal with multiple new_keys
            new_keys = []
            values = []
            if isinstance(new_key, dict):  # when schema is not 1.0
                _new_key_keys = new_key.keys()
                new_keys = new_key.values()
                values = []
                for _key in _new_key_keys:
                    values.append(value.get(_key))
            else:
                new_keys.append(new_key)
                values.append(value)

            if not any(item for item in values):
                continue

            mini_dataset = dict(zip(new_keys, values))
            for mini_key, mini_value in mini_dataset.iteritems():
                if not mini_value:
                    continue
                if mini_key.startswith('extras__'):
                    extras.append({"key": mini_key[8:], "value": mini_value})
                else:
                    pkg[mini_key] = mini_value

        # pick a fix number of unmapped entries and put into extra
        if unmapped:
            unmapped.sort()
            del unmapped[100:]
            for key in unmapped:
                value = dataset_processed.get(key, "")
                if value is not None: extras.append({"key": key, "value": value})

        # if theme is geospatial/Geospatial, we tag it in metadata_type.
        themes = self.find_extra(pkg, "theme")
        if themes and ('geospatial' in [x.lower() for x in themes]):
            extras.append({'key': 'metadata_type', 'value': 'geospatial'})

        if is_collection:
            extras.append({'key': 'collection_metadata', 'value': 'true'})
        elif parent_pkg_id:
            extras.append(
                {'key': 'collection_package_id', 'value': parent_pkg_id}
            )

        for k, v in catalog_extras.iteritems():
            extras.append({'key': k, 'value': v})

        # Set specific information about the dataset.
        self.set_dataset_info(pkg, dataset_processed, dataset_defaults, schema_version)

        # Try to update an existing package with the ID set in harvest_object.guid. If that GUID
        # corresponds with an existing package, get its current metadata.
        try:
            existing_pkg = get_action('package_show')(self.context(), {"id": harvest_object.guid})
        except NotFound:
            existing_pkg = None

        if existing_pkg:
            # Update the existing metadata with the new information.

            # But before doing that, try to avoid replacing existing resources with new resources
            # my assigning resource IDs where they match up.
            for res in pkg.get("resources", []):
                for existing_res in existing_pkg.get("resources", []):
                    if res["url"] == existing_res["url"]:
                        res["id"] = existing_res["id"]
            pkg['groups'] = existing_pkg['groups']
            existing_pkg.update(pkg)  # preserve other fields that we're not setting, but clobber extras
            pkg = existing_pkg

            log.warn('updating package %s (%s) from %s' % (pkg["name"], pkg["id"], harvest_object.source.url))
            pkg = get_action('package_update')(self.context(), pkg)
        else:
            # It doesn't exist yet. Create a new one.
            pkg['name'] = self.make_package_name(dataset_processed["title"], harvest_object.guid)
            try:
                pkg = get_action('package_create')(self.context(), pkg)
                log.warn('created package %s (%s) from %s' % (pkg["name"], pkg["id"], harvest_object.source.url))
            except IntegrityError:
                # sometimes one fetch worker does not see new pkg added
                # by other workers. it gives db error for pkg with same title.
                model.Session.rollback()
                pkg['name'] = self.make_package_name(dataset_processed["title"], harvest_object.guid)
                pkg = get_action('package_create')(self.context(), pkg)
                log.warn('created package %s (%s) from %s' % (pkg["name"], pkg["id"], harvest_object.source.url))
            except:
                log.error('failed to create package %s from %s' % (pkg["name"], harvest_object.source.url))
                raise

        # Flag the other HarvestObjects linking to this package as not current anymore
        for ob in model.Session.query(HarvestObject).filter_by(package_id=pkg["id"]):
            ob.current = False
            ob.save()

        # Flag this HarvestObject as the current harvest object
        harvest_object.package_id = pkg['id']
        harvest_object.current = True
        harvest_object.save()
        model.Session.commit()

        # Now that the package and the harvest source are associated, re-index the
        # package so it knows it is part of the harvest source. The CKAN harvester
        # does this by creating the association before the package is saved by
        # overriding the GUID creation on a new package. That's too difficult.
        # So here we end up indexing twice.
        PackageSearchIndex().index_package(pkg)

        return True

    def make_upstream_content_hash(self, datasetdict, harvest_source,
                                   catalog_extras, schema_version='1.0'):
        if schema_version == '1.0':
            return hashlib.sha1(json.dumps(datasetdict, sort_keys=True)
                                + "|" + harvest_source.config + "|"
                                + self.HARVESTER_VERSION).hexdigest()
        else:
            return hashlib.sha1(json.dumps(datasetdict, sort_keys=True)
                                + "|" + json.dumps(catalog_extras,
                                                   sort_keys=True)).hexdigest()

    def find_extra(self, pkg, key):
        for extra in pkg["extras"]:
            if extra["key"] == key:
                return extra["value"]
        return None

    def make_package_name(self, title, exclude_existing_package):
        '''
        Creates a URL friendly name from a title

        If the name already exists, it will add some random characters at the end
        '''

        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        name = name[0:90]  # max length is 100

        # Is this slug already in use (and if we're updating a package, is it in
        # use by a different package?).
        pkg_obj = Session.query(Package).filter(Package.name == name).filter(
            Package.id != exclude_existing_package).first()
        if not pkg_obj:
            # The name is available, so use it. Note that if we're updating an
            # existing package we will be updating this package's URL, so incoming
            # links may break.
            return name

        if exclude_existing_package:
            # The name is not available, and we're updating a package. Chances
            # are the package's name already had some random string attached
            # to it last time. Prevent spurrious updates to the package's URL
            # (choosing new random text) by just reusing the existing package's
            # name.
            pkg_obj = Session.query(Package).filter(Package.id == exclude_existing_package).first()
            if pkg_obj:  # the package may not exist yet because we may be passed the desired package GUID before a new package is instantiated
                return pkg_obj.name

        # Append some random text to the URL. Hope that with five character
        # there will be no collsion.
        return name + "-" + str(uuid.uuid4())[:5]
