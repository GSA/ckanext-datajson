
import io
import json
import logging
import os
import sys
from builtins import str
from re import L

# from pylons import request, response
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.model as model
import ckan.plugins as p
import six
from ckan.common import c
from ckan.plugins.toolkit import render, request
from flask import Blueprint
from flask.wrappers import Response
from jsonschema.exceptions import best_match

import plugin

from .helpers import detect_publisher, get_export_map_json, get_validator
from .package2pod import Package2Pod

datapusher = Blueprint('datajson', __name__)


logger = logging.getLogger(__name__)
draft4validator = get_validator()
_errors_json = []
_zip_name = ''


def generate_json():
    return generate_output(org_id=None)


def generate_org_json(org_id):
    return generate_output(org_id=org_id)


# def generate_jsonld():
#     return generate_output('json-ld')


def generate_redacted(org_id):
    return generate('redacted', org_id=org_id)


def generate_unredacted(org_id):
    return generate('unredacted', org_id=org_id)


def generate_draft(org_id):
    return generate('draft', org_id=org_id)


def generate(export_type='datajson', org_id=None):
    logger.debug('Generating JSON for {} to {} ({})'.format(export_type, org_id, c.user))

    if export_type not in ['draft', 'redacted', 'unredacted']:
        return "Invalid type, Assigned type: %s" % (export_type)
    if org_id is None:
        return "Invalid organization id"

    # If user is not editor or admin of the organization then don't allow unredacted download
    try:
        auth = p.toolkit.check_access('package_create',
                                    {'model': model, 'user': c.user},
                                    {'owner_org': org_id}
                                    )
    except p.toolkit.NotAuthorized:
        logger.error('NotAuthorized to generate JSON for {} to {} ({})'.format(export_type, org_id, c.user))
        auth = False

    if not auth:
        return "Not Authorized"

    # set content type (charset required or pylons throws an error)
    Response.content_type = 'application/json; charset=UTF-8'

    # allow caching of response (e.g. by Apache)
    # Commented because it works without it
    del Response.headers["Cache-Control"]
    del Response.headers["Pragma"]
    #resp = Response(make_json(export_type, org_id), mimetype='application/octet-stream')
    #resp.headers['Content-Disposition'] = 'attachment; filename="%s.zip"' % _zip_name

    return make_json(export_type, org_id)


def generate_output(org_id=None):
    global _errors_json
    _errors_json = []
    # set content type (charset required or pylons throws an error)
    Response.content_type = 'application/json; charset=UTF-8'

    # allow caching of response (e.g. by Apache)
    # Commented because it works without it
    # del Response.headers["Cache-Control"]
    # del Response.headers["Pragma"]

    # TODO special processing for enterprise
    # output
    data = make_json(export_type='datajson', owner_org=org_id)
    return p.toolkit.literal(json.dumps(data))


def make_json(export_type='datajson', owner_org=None, with_private=False):
    # Error handler for creating error log
    stream = io.StringIO()
    eh = logging.StreamHandler(stream)
    eh.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    eh.setFormatter(formatter)
    logger.addHandler(eh)

    data = ''
    output = []
    errors_json = []
    Package2Pod.seen_identifiers = set()

    # first_line = True

    try:
        n = 500
        page = 1
        dataset_list = []
        q = '+capacity:public' if not with_private else '*:*'
        fq = 'dataset_type:dataset'
        # Build the data.json file.
        if owner_org:
            fq += "AND organization:" + owner_org
        
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

            if len(query['results']):
                json_export_map = get_export_map_json('export.map.json')

                if json_export_map:
                    for pkg in packages:
                        if json_export_map.get('debug'):
                            output.append(pkg)
                        extras = dict([(x['key'], x['value']) for x in pkg.get('extras', {})])

                        if export_type in ['unredacted', 'redacted']:
                            if 'Draft' == extras.get('publishing_status'):
                                continue
                        elif 'draft' == export_type:
                            if 'publishing_status' not in extras.keys() or extras.get('publishing_status') != 'Draft':
                                continue
                        
                        redaction_enabled = 'redacted' == export_type
                        datajson_entry = Package2Pod.convert_package(pkg, json_export_map, plugin.DataJsonPlugin.site_url, redaction_enabled)
                        
                        errors = None
                        if 'errors' in datajson_entry.keys():
                            errors_json.append(datajson_entry)
                            errors = datajson_entry.get('errors')
                            datajson_entry = None
                        
                        if datajson_entry and \
                                    (not json_export_map.get('validation_enabled') or is_valid(datajson_entry)):
                                output.append(datajson_entry)
                        else:
                            publisher = detect_publisher(extras)
                            if errors:
                                logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason below:\n\t%s\n",
                                            pkg.get('id', None), pkg.get('title', None), pkg.get('organization').get('title'), errors)
                            else:
                                logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason above.\n",
                                            pkg.get('id', None), pkg.get('title', None), pkg.get('organization').get('title'))
                    if 'datajson' == export_type:
                        page +=1
                    else:
                        break
            else:
                break
        data = Package2Pod.wrap_json_catalog(output, json_export_map)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, str(e))

    # Get the error log
    eh.flush()
    error = stream.getvalue()
    eh.close()
    logger.removeHandler(eh)
    stream.close()

    # Skip compression if we export whole /data.json catalog
    return data


def get_packages(owner_org, with_private=True):
    # Build the data.json file.
    packages = get_all_group_packages(group_id=owner_org, with_private=with_private)
    # get packages for sub-agencies.
    sub_agency = model.Group.get(owner_org)

    if six.PY2:
        if 'sub-agencies' in sub_agency.extras.col.target \
                and sub_agency.extras.col.target['sub-agencies'].state == 'active':
            sub_agencies = sub_agency.extras.col.target['sub-agencies'].value
            sub_agencies_list = sub_agencies.split(",")
            for sub in sub_agencies_list:
                sub_packages = get_all_group_packages(group_id=sub, with_private=with_private)
                for sub_package in sub_packages:
                    packages.append(sub_package)
    else:
        if 'sub-agencies' in sub_agency.extras.col.keys() \
                and sub_agency.extras.col['sub-agencies'].state == 'active':
            sub_agencies = sub_agency.extras.col['sub-agencies'].value
            sub_agencies_list = sub_agencies.split(",")
            for sub in sub_agencies_list:
                sub_packages = get_all_group_packages(group_id=sub, with_private=with_private)
                for sub_package in sub_packages:
                    packages.append(sub_package)

    return packages


def get_all_group_packages(group_id, with_private=True):
    """
    Gets all of the group packages, public or private, returning them as a list of CKAN's dictized packages.
    """
    result = []

    for pkg_rev in model.Group.get(group_id).packages(with_private=with_private, context={'user_is_admin': True}):
        result.append(model_dictize.package_dictize(pkg_rev, {'model': model}))

    return result


def is_valid(instance):
    """
    Validates a data.json entry against the DCAT_US JSON schema.
    Log a warning message on validation error
    """
    error = best_match(draft4validator.iter_errors(instance))
    if error:
        logger.warn(("===================================================\r\n"
                    "Validation failed, best guess of error:\r\n %s\r\nFor this dataset:\r\n"), error)
        return False
    return True


def write_zip(data, error=None, errors_json=None, zip_name='data'):
    """
    Data: a python object to write to the data.json
    Error: unicode string representing the content of the error log.
    zip_name: the name to use for the zip file
    """
    import zipfile
    global _errors_json, _zip_name

    o = io.BytesIO()
    zf = zipfile.ZipFile(o, mode='w')

    _data_file_name = 'data.json'
    _zip_name = zip_name
    if 'draft' == zip_name:
        _data_file_name = 'draft_data.json'

    # Write the data file
    if data:
        if sys.version_info >= (3, 0):
            zf.writestr(_data_file_name, json.dumps(data))
        else:
            zf.writestr(_data_file_name, json.dumps(data))

    # Write empty.json if nothing to return
    else:
        # logger.debug('no data to write')
        zf.writestr('empty.json', '')

    if _errors_json:
        if errors_json:
            errors_json += _errors_json
        else:
            errors_json = _errors_json

    # Errors in json format
    if errors_json:
        # logger.debug('writing errors.json')
        zf.writestr('errors.json', json.dumps(errors_json).encode('utf8'))

    # Write the error log
    if error:
        # logger.debug('writing errorlog.txt')
        zf.writestr('errorlog.txt', error.encode('utf8').replace("\n", "\r\n"))

    zf.close()
    o.seek(0)

    binary = o.read()
    o.close()

    return binary


def validator():
    # Validates that a URL is a good data.json file.
    if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
        c.source_url = request.POST["url"]
        c.errors = []

        import json
        import urllib.error
        import urllib.parse
        import urllib.request

        from .datajsonvalidator import do_validation

        body = None
        try:
            body = json.load(urllib.request.urlopen(c.source_url))
        except IOError as e:
            c.errors.append(("Error Loading File", ["The address could not be loaded: " + str(e)]))
        except ValueError as e:
            c.errors.append(("Invalid JSON", ["The file does not meet basic JSON syntax requirements: " + str(
                e) + ". Try using JSONLint.com."]))
        except Exception as e:
            c.errors.append((
                "Internal Error",
                ["Something bad happened while trying to load and parse the file: " + str(e)]))

        if body:
            try:
                do_validation(body, c.errors)
            except Exception as e:
                c.errors.append(("Internal Error", ["Something bad happened: " + str(e)]))
            if len(c.errors) == 0:
                c.errors.append(("No Errors", ["Great job!"]))

    return render('datajsonvalidator.html')


def test_for_gil():
    logger.info('\n\n\n\n\n\n\n GIL TESTED HERE \n\n\n\n\n\n\n')


datapusher.add_url_rule('/internal/data.json',
                        view_func=generate_json)
datapusher.add_url_rule('/organization/<org_id>/data.json',
                        view_func=generate_org_json)
datapusher.add_url_rule('/organization/<org_id>/redacted.json',
                        view_func=generate_redacted)
datapusher.add_url_rule('/organization/<org_id>/unredacted.json',
                        view_func=generate_unredacted)
datapusher.add_url_rule('/organization/<org_id>/draft.json',
                        view_func=generate_draft)
datapusher.add_url_rule("/pod/validate",
                        view_func=validator)
datapusher.add_url_rule("/test/gil", view_func=test_for_gil)
