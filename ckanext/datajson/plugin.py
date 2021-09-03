import StringIO
import json
import logging
import sys

import ckan.lib.dictization.model_dictize as model_dictize
import ckan.model as model
import ckan.plugins as p
import os
import re
from ckan.lib.base import BaseController, render, c
from jsonschema.exceptions import best_match
from pylons import request, response
from helpers import get_export_map_json, detect_publisher, get_validator
from package2pod import Package2Pod

logger = logging.getLogger(__name__)

try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict


class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.ITemplateHelpers)
    p.implements(p.interfaces.IRoutes, inherit=True)

    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # DataJsonPlugin.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True") == 'True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/internal/data.json")
        #DataJsonPlugin.cache_path = config.get("ckanext.datajson.cache_path", "/internal/data.json")
        # DataJsonPlugin.cache_serve = config.get("ckanext.datajson.cache_serve", "/dcat-us/site_data.json")
        #DataJsonPlugin.route_path = config.get("ckanext.datajson.cache_serve", "/dcat-us/site_data.json")
        DataJsonPlugin.route_ld_path = config.get("ckanext.datajsonld.path",
                                                  re.sub(r"\.json$", ".jsonld", DataJsonPlugin.route_path))
        DataJsonPlugin.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        DataJsonPlugin.ld_title = config.get("ckan.site_title", "Catalog")
        DataJsonPlugin.site_url = config.get("ckan.site_url")
        DataJsonPlugin.schema_type = config.get("ckanext.datajson.schema_type", "federal-v1.1")

        DataJsonPlugin.inventory_links_enabled = config.get("ckanext.datajson.inventory_links_enabled",
                                                            "False") == 'True'

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "templates")

    @staticmethod
    def datajson_inventory_links_enabled():
        return DataJsonPlugin.inventory_links_enabled

    def get_helpers(self):
        return {
            'datajson_inventory_links_enabled': self.datajson_inventory_links_enabled
        }

    def before_map(self, m):
        return m

    # TODO: Add redirect for organization/org_id/cache_data.json to return a 200 response letting the user know
    # that the data.json is being created
    def after_map(self, m):
        if DataJsonPlugin.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)

            # Have 2 routes: 1 to create the json file (and store locally),
            # the other to return the local file
            m.connect('datajson_export', DataJsonPlugin.route_path,
                    controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')

            m.connect('organization_export', '/organization/{org_id}/data.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_org_json')

        if DataJsonPlugin.inventory_links_enabled:
            m.connect('public_data_listing', '/organization/{org_id}/redacted.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_redacted')

            m.connect('enterprise_data_inventory', '/organization/{org_id}/unredacted.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_unredacted')

            m.connect('enterprise_data_inventory', '/organization/{org_id}/draft.json',
                      controller='ckanext.datajson.plugin:DataJsonController', action='generate_draft')

        # /pod/validate
        m.connect('datajsonvalidator', "/pod/validate",
                  controller='ckanext.datajson.plugin:DataJsonController', action='validator')

        return m


class DataJsonController(BaseController):
    _errors_json = []

    def generate_json(self):
        return self.generate_output(org_id=None)

    def generate_org_json(self, org_id):
        return self.generate_output(org_id=org_id)

    def generate_redacted(self, org_id):
        return self.generate('redacted', org_id=org_id)

    def generate_unredacted(self, org_id):
        return self.generate('unredacted', org_id=org_id)

    def generate_draft(self, org_id):
        return self.generate('draft', org_id=org_id)

    def generate(self, export_type='datajson', org_id=None):
        logger.debug('Generating JSON for {} to {} ({})'.format(export_type, org_id, c.user))

        if export_type not in ['draft', 'redacted', 'unredacted']:
            return "Invalid type"
        if org_id is None:
            return "Invalid organization id"

        # If user is not editor or admin of the organization then don't allow unredacted download
        try: 
            auth = p.toolkit.check_access(
                        'package_create',
                        {'model': model, 'user': c.user},
                        {'owner_org': org_id}
                        )
        except p.toolkit.NotAuthorized:
            logger.error('NotAuthorized to generate JSON for {} to {} ({})'.format(export_type, org_id, c.user))
            auth = False
        
        if not auth:
            return "Not Authorized"

        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]
        return self.make_json(export_type, org_id)

    def generate_output(self, org_id=None):
        self._errors_json = []
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        # TODO special processing for enterprise
        # output
        data = self.make_json(export_type='datajson', owner_org=org_id)

        # if org_id is None:
        #     #TODO: Bring this code (most of it) into the make_json method, get this working with org path as well
        #     data_json_file = open('site_data.json', 'w')
        #     #data_json_file.write(json.dumps(data, indent=2))
        #     #json_iterate(jsonDict=dict(data), data_json_file)
        #     data_json_file.close()

        return p.toolkit.literal(json.dumps(data))
        #return p.toolkit.literal(json.dumps(self.make_json(export_type='datajson', owner_org=org_id)))

    def make_json(self, export_type='datajson', owner_org=None, with_private=False):
        # """
        # file_path = 'site_data.json'
        # if owner_org is not None and owner_org != 'None':
        #     file_path = '{}_data.json'.format(owner_org)

        # """
        #file_path = 'datajson/site_data.json'
        
        #if owner_org is not None and owner_org != 'None':
        #    file_path = 'datajson/{}_data.json'.format(owner_org)

        #logger.info('\n\n\n'+file_path+'\n\n\n')
        # if owner_org is not None and owner_org != 'None': 
        #     data_json_file = open('{}_data.json'.format(owner_org), mode="w")
        #     data_json_file.close()
        #     data_json_file = open('{}_data.json'.format(owner_org), 'a')
        #     logger.info('\n\n\n\nMaking data json\n\n\n')
        # else:
        #     data_json_file = open('site_data.json', 'w')
        #     data_json_file.close()
        #     data_json_file = open('site_data.json', 'a')
        #     logger.info('\n\n\n\nMaking data json\n\n\n')

        #data_json_file = open(file_path, mode='w')
        #data_json_file.close()
        #data_json_file = open(file_path, mode='a')

        # response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        # del response.headers["Cache-Control"]
        # del response.headers["Pragma"]

        logger.info('\n{}\n'.format(owner_org))
        
        # Error handler for creating error log
        stream = StringIO.StringIO()
        eh = logging.StreamHandler(stream)
        eh.setLevel(logging.WARN)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        eh.setFormatter(formatter)
        logger.addHandler(eh)

        data = ''
        output = []
        first_line = True
        
        
        errors_json = []
        Package2Pod.seen_identifiers = set()

        try:
            # Build the data.json file.
            # if owner_org:
            #     if 'datajson' == export_type:
            #         # we didn't check ownership for this type of export, so never load private datasets here
            #         packages = DataJsonController._get_ckan_datasets(org=owner_org)
            #         if not packages:
            #             packages = self.get_packages(owner_org=owner_org, with_private=False)
            #     else:
            #         packages = self.get_packages(owner_org=owner_org, with_private=True)
            # else:
            #     # TODO: load data by pages
            #     # packages = p.toolkit.get_action("current_package_list_with_resources")(
            #     # None, {'limit': 50, 'page': 300})
            #     packages = DataJsonController._get_ckan_datasets()
            #     # packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})
            n = 500
            page = 1
            dataset_list = []
            q = '+capacity:public' if not with_private else '*:*'
            fq = 'dataset_type:dataset'
            if owner_org:
                fq += " AND organization:" + owner_org
            while True:
                search_data_dict = {
                    'q': q,
                    'fq': fq,
                    'sort': 'metadata_modified desc',
                    'rows': n,
                    'start': n * (page - 1),
                }
                query = p.toolkit.get_action('package_search')({}, search_data_dict)
                packages = query['results']
                """ if owner_org:
                    if 'datajson' == export_type:
                        if not packages:
                            packages = self.get_packages(owner_org=owner_org, with_private=False) """
                if len(query['results']):
                    json_export_map = get_export_map_json('export.map.json')
                    #data = Package2Pod.wrap_json_catalog(output, json_export_map)
                    #if first_line:
                    #    output.append(data)

                    if json_export_map:
                        for pkg in packages:
                            if json_export_map.get('debug'):
                                output.append(pkg)
                            # logger.error('package: %s', json.dumps(pkg))
                            # logger.debug("processing %s" % (pkg.get('title')))
                            extras = dict([(x['key'], x['value']) for x in pkg.get('extras', {})])

                            # unredacted = all non-draft datasets (public + private)
                            # redacted = public-only, non-draft datasets
                            if export_type in ['unredacted', 'redacted']:
                                if 'Draft' == extras.get('publishing_status'):
                                    # publisher = detect_publisher(extras)
                                    # logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted (%s)\n",
                                    #             pkg.get('id'), pkg.get('title'), publisher,
                                    #             'publishing_status: Draft')
                                    # self._errors_json.append(OrderedDict([
                                    #     ('id', pkg.get('id')),
                                    #     ('name', pkg.get('name')),
                                    #     ('title', pkg.get('title')),
                                    #     ('errors', [(
                                    #         'publishing_status: Draft',
                                    #         [
                                    #             'publishing_status: Draft'
                                    #         ]
                                    #     )])
                                    # ]))

                                    continue
                                    # if 'redacted' == export_type and re.match(r'[Nn]on-public', extras.get('public_access_level')):
                                    #     continue
                            # draft = all draft-only datasets
                            elif 'draft' == export_type:
                                if 'publishing_status' not in extras.keys() or extras.get('publishing_status') != 'Draft':
                                    continue

                            redaction_enabled = ('redacted' == export_type)
                            datajson_entry = Package2Pod.convert_package(pkg, json_export_map, DataJsonPlugin.site_url, redaction_enabled)
                            #output.append(datajson_entry)

                            errors = None
                            if 'errors' in datajson_entry.keys():
                                errors_json.append(datajson_entry)
                                errors = datajson_entry.get('errors')
                                datajson_entry = None

                            if datajson_entry and \
                                    (not json_export_map.get('validation_enabled') or self.is_valid(datajson_entry)):
                                # logger.debug("writing to json: %s" % (pkg.get('title')))
                                #TODO: Write data_json entry to file instead of array
                                #output.append(datajson_entry)
                                
                                output.append(datajson_entry)
                              #  else:
                              #      data_json_file.write(json.dumps(datajson_entry, indent=2))
                            else:
                                publisher = detect_publisher(extras)
                                if errors:
                                    logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason below:\n\t%s\n",
                                                pkg.get('id', None), pkg.get('title', None), pkg.get('organization').get('title'), errors)
                                else:
                                    logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason above.\n",
                                                pkg.get('id', None), pkg.get('title', None), pkg.get('organization').get('title'))
                        if 'datajson' == export_type:
                            page += 1
                        else:
                            break
                        # TODO: call this with empty array, strip out last bracket and curly brace, write this to file first
                else:
                    # data_json_file.write(']}')
                    break 
            data = Package2Pod.wrap_json_catalog(output, json_export_map)       
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, unicode(e))

        # Get the error log
        eh.flush()
        error = stream.getvalue()
        eh.close()
        logger.removeHandler(eh)
        stream.close()

        # Skip compression if we export whole /data.json catalog
        # with open(file_path, mode='r+') as data_json_file:
        #    # logger.info(data_json_file.read())
        #     data_json_file.seek(0, os.SEEK_END)
        #     pos = data_json_file.tell() - 3
        #     data_json_file.seek(pos, os.SEEK_SET)
        #     data_json_file.truncate()
        #     #check for last two characters
        #     data_json_file.write(']}')

        # data_json_file.close()

        # data_json_file = open(file_path, mode='r')
        #json_vals = data_json_file.read()
        #json_vals = json_vals[0:-3] + '' + json_vals[-2:]

        # return p.toolkit.literal(data_json_file.read())
        return data


    def get_packages(self, owner_org, with_private=True):
        # Build the data.json file.
        packages = self.get_all_group_packages(group_id=owner_org, with_private=with_private)
        # get packages for sub-agencies.
        sub_agency = model.Group.get(owner_org)
        if 'sub-agencies' in sub_agency.extras.col.target \
                and sub_agency.extras.col.target['sub-agencies'].state == 'active':
            sub_agencies = sub_agency.extras.col.target['sub-agencies'].value
            sub_agencies_list = sub_agencies.split(",")
            for sub in sub_agencies_list:
                sub_packages = self.get_all_group_packages(group_id=sub, with_private=with_private)
                for sub_package in sub_packages:
                    packages.append(sub_package)

        return packages

    def get_all_group_packages(self, group_id, with_private=True):
        """
        Gets all of the group packages, public or private, returning them as a list of CKAN's dictized packages.
        """
        result = []

        for pkg_rev in model.Group.get(group_id).packages(with_private=with_private, context={'user_is_admin': True}):
            result.append(model_dictize.package_dictize(pkg_rev, {'model': model}))

        return result

    def is_valid(self, instance):
        """
        Validates a data.json entry against the DCAT_US JSON schema.
        Log a warning message on validation error
        """
        error = best_match(get_validator(schema_type=DataJsonPlugin.schema_type).iter_errors(instance))
        if error:
            logger.warn("===================================================\r\n"+
                        "Validation failed, best guess of error:\r\n %s\r\nFor this dataset:\r\n", error)
            return False
        return True

    def write_zip(self, data, error=None, errors_json=None, zip_name='data'):
        """
        Data: a python object to write to the data.json
        Error: unicode string representing the content of the error log.
        zip_name: the name to use for the zip file
        """
        import zipfile

        o = StringIO.StringIO()
        zf = zipfile.ZipFile(o, mode='w')

        data_file_name = 'data.json'
        if 'draft' == zip_name:
            data_file_name = 'draft_data.json'

        # Write the data file
        if data:
            zf.writestr(data_file_name, json.dumps(data, ensure_ascii=False).encode('utf8'))

        # Write empty.json if nothing to return
        else:
            # logger.debug('no data to write')
            zf.writestr('empty.json', '')

        if self._errors_json:
            if errors_json:
                errors_json += self._errors_json
            else:
                errors_json = self._errors_json

        # Errors in json format
        if errors_json:
            # logger.debug('writing errors.json')
            zf.writestr('errors.json', json.dumps(errors_json).encode('utf8'))

        # Write the error log
        if error:
            # logger.debug('writing errorlog.txt')
            zf.writestr('errorlog.txt', error.encode('utf8').replace("\n","\r\n"))

        zf.close()
        o.seek(0)

        binary = o.read()
        o.close()

        response.content_type = 'application/octet-stream'
        response.content_disposition = 'attachment; filename="%s.zip"' % zip_name

        return binary

    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.errors = []

            import urllib
            import json
            from datajsonvalidator import do_validation

            body = None
            try:
                body = json.load(urllib.urlopen(c.source_url))
            except IOError as e:
                c.errors.append(("Error Loading File", ["The address could not be loaded: " + unicode(e)]))
            except ValueError as e:
                c.errors.append(("Invalid JSON", ["The file does not meet basic JSON syntax requirements: " + unicode(
                    e) + ". Try using JSONLint.com."]))
            except Exception as e:
                c.errors.append((
                    "Internal Error",
                    ["Something bad happened while trying to load and parse the file: " + unicode(e)]))

            if body:
                try:
                    do_validation(body, c.errors)
                except Exception as e:
                    c.errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
                if len(c.errors) == 0:
                    c.errors.append(("No Errors", ["Great job!"]))

        return render('datajsonvalidator.html')