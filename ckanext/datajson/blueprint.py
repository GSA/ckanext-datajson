
from builtins import str
import io
import json
import logging
import six
import sys

from ckan.common import c
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.model as model
import ckan.plugins as p
from ckan.plugins.toolkit import request, render
from flask.wrappers import Response
from flask import Blueprint
from jsonschema.exceptions import best_match
import os

from .helpers import get_export_map_json, detect_publisher, get_validator
from .package2pod import Package2Pod


datapusher = Blueprint('datajson', __name__)


logger = logging.getLogger(__name__)
draft4validator = get_validator()
_errors_json = []
_zip_name = ''


def generate_json():
    return generate_output('json')


def generate_org_json(org_id):
    return generate_output('json', org_id=org_id)


# def generate_jsonld():
#     return generate_output('json-ld')


def generate_redacted(org_id):
    return generate('redacted', org_id=org_id)


def generate_unredacted(org_id):
    return generate('unredacted', org_id=org_id)


def generate_draft(org_id):
    return generate('draft', org_id=org_id)


def generate(export_type='datajson', org_id=None):
    """ generate a JSON response """
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
    # del Response.headers["Cache-Control"]
    # del Response.headers["Pragma"]
    resp = Response(make_json(export_type, org_id), mimetype='application/octet-stream')
    resp.headers['Content-Disposition'] = 'attachment; filename="%s.zip"' % _zip_name

    return resp


def generate_output(fmt='json', org_id=None):
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

    # if fmt == 'json-ld':
    #     # Convert this to JSON-LD.
    #     data = OrderedDict([
    #         ("@context", OrderedDict([
    #             ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    #             ("dcterms", "http://purl.org/dc/terms/"),
    #             ("dcat", "http://www.w3.org/ns/dcat#"),
    #             ("foaf", "http://xmlns.com/foaf/0.1/"),
    #         ])),
    #         ("@id", DataJsonPlugin.ld_id),
    #         ("@type", "dcat:Catalog"),
    #         ("dcterms:title", DataJsonPlugin.ld_title),
    #         ("rdfs:label", DataJsonPlugin.ld_title),
    #         ("foaf:homepage", DataJsonPlugin.site_url),
    #         ("dcat:dataset", [dataset_to_jsonld(d) for d in data.get('dataset')]),
    #     ])

    return p.toolkit.literal(json.dumps(data, indent=2))


def make_json(export_type='datajson', owner_org=None):
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

    try:
        # Build the data.json file.
        if owner_org:
            if 'datajson' == export_type:
                # we didn't check ownership for this type of export, so never load private datasets here
                packages = _get_ckan_datasets(org=owner_org)
                if not packages:
                    packages = get_packages(owner_org=owner_org, with_private=False)
            else:
                packages = get_packages(owner_org=owner_org, with_private=True)
        else:
            # TODO: load data by pages
            # packages = p.toolkit.get_action("current_package_list_with_resources")(
            # None, {'limit': 50, 'page': 300})
            packages = _get_ckan_datasets()
            # packages = p.toolkit.get_action("current_package_list_with_resources")(None, {})

        json_export_map = get_export_map_json('export.map.json')

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
                        # _errors_json.append(OrderedDict([
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
                        # if ('redacted' == export_type and
                        #         re.match(r'[Nn]on-public', extras.get('public_access_level'))):
                        #     continue
                # draft = all draft-only datasets
                elif 'draft' == export_type:
                    if 'publishing_status' not in list(extras.keys()) or extras.get('publishing_status') != 'Draft':
                        continue

                redaction_enabled = ('redacted' == export_type)
                datajson_entry = Package2Pod.convert_package(pkg, json_export_map, redaction_enabled)
                errors = None
                if 'errors' in list(datajson_entry.keys()):
                    errors_json.append(datajson_entry)
                    errors = datajson_entry.get('errors')
                    datajson_entry = None

                if datajson_entry and \
                        (not json_export_map.get('validation_enabled') or is_valid(datajson_entry)):
                    # logger.debug("writing to json: %s" % (pkg.get('title')))
                    output.append(datajson_entry)
                else:
                    publisher = detect_publisher(extras)
                    if errors:
                        logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason below:\n\t%s\n",
                                    pkg.get('id', None), pkg.get('title', None), publisher, errors)
                    else:
                        logger.warn("Dataset id=[%s], title=[%s], organization=[%s] omitted, reason above.\n",
                                    pkg.get('id', None), pkg.get('title', None), publisher)

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
    if 'datajson' == export_type:
        return data

    return write_zip(data, error, errors_json, zip_name=export_type)


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

        import urllib.request
        import urllib.parse
        import urllib.error
        import json
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


def _get_ckan_datasets(org=None, with_private=False):
    n = 500
    page = 1
    dataset_list = []

    q = '+capacity:public' if not with_private else '*:*'

    fq = 'dataset_type:dataset'
    if org:
        fq += " AND organization:" + org

    while True:
        search_data_dict = {
            'q': q,
            'fq': fq,
            'sort': 'metadata_modified desc',
            'rows': n,
            'start': n * (page - 1),
        }

        query = p.toolkit.get_action('package_search')({}, search_data_dict)
        if len(query['results']):
            dataset_list.extend(query['results'])
            page += 1
        else:
            break
    return dataset_list


datapusher.add_url_rule('/data.json',
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
