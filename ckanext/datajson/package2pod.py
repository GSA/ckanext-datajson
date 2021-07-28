try:
    from collections import OrderedDict  # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

import string
import os
import sys
import json
import ckan.plugins.toolkit as tk
import collections

from logging import getLogger

from helpers import *
from ckan import logic
from ckan import model
import ckan.plugins as p

log = getLogger(__name__)


class Package2Pod:
    def __init__(self):
        pass

    seen_identifiers = None

    @staticmethod
    def wrap_json_catalog(dataset_dict, json_export_map):
        catalog_headers = [(x, y) for x, y in json_export_map.get('catalog_headers').iteritems()]
        catalog = OrderedDict(
            catalog_headers + [('dataset', dataset_dict)]
        )
        return catalog

    @staticmethod
    def filter(content):
        if not isinstance(content, (str, unicode)):
            return content
        content = Package2Pod.strip_redacted_tags(content)
        content = strip_if_string(content)
        return content

    @staticmethod
    def strip_redacted_tags(content):
        if not isinstance(content, (str, unicode)):
            return content
        return re.sub(REDACTED_TAGS_REGEX, '', content)

    @staticmethod
    def mask_redacted(content, reason):
        if not content:
            content = ''
        if reason:
            # check if field is partial redacted
            masked = content
            for redact in re.findall(PARTIAL_REDACTION_REGEX, masked):
                masked = masked.replace(redact, '')
            if len(masked) < len(content):
                return masked
            return '[[REDACTED-EX ' + reason + ']]'
        return content

    @staticmethod
    def convert_package(package, json_export_map, redaction_enabled=False):
        import sys, os

        try:
            dataset = Package2Pod.export_map_fields(package, json_export_map, redaction_enabled)

            # skip validation if we export whole /data.json catalog
            if json_export_map.get('validation_enabled'):
                return Package2Pod.validate(package, dataset)
            else:
                return dataset
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, unicode(e))
            raise e

    @staticmethod
    def export_map_fields(package, json_export_map, redaction_enabled=False):
        # If already harvested DCAT-US format, it should already be validated; use original
        try:
            harvest_object_id = get_extra(package, 'harvest_object_id')
            bureau_code = get_extra(package, 'bureau_code')
            # If harvested and has bureau code (only DCAT-US has this metadata field)
            if harvest_object_id and bureau_code:
                harvest_obj = tk.get_action('harvest_object_show')({}, {'id': harvest_object_id})
                parsed_obj = json.loads(harvest_obj['content'], object_pairs_hook=collections.OrderedDict)
                return parsed_obj
        except Exception as e:
            # Something went wrong getting the harvest object, log and attempt package build
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, unicode(e))

        public_access_level = get_extra(package, 'public_access_level')
        if not public_access_level or public_access_level not in ['non-public', 'restricted public']:
            redaction_enabled = False

        Wrappers.redaction_enabled = redaction_enabled

        json_fields = json_export_map.get('dataset_fields_map')

        try:
            dataset = OrderedDict([("@type", "dcat:Dataset")])

            Wrappers.pkg = package
            Wrappers.full_field_map = json_fields

            for key, field_map in json_fields.iteritems():
                # log.debug('%s => %s', key, field_map)

                field_type = field_map.get('type', 'direct')
                is_extra = field_map.get('extra')
                array_key = field_map.get('array_key')
                field = field_map.get('field')
                split = field_map.get('split')
                wrapper = field_map.get('wrapper')
                default = field_map.get('default')

                if redaction_enabled and field and 'publisher' != field and 'direct' != field_type:
                    redaction_reason = get_extra(package, 'redacted_' + field, False)
                    # keywords(tags) have some UI-related issues with this, so we'll check both versions here
                    if not redaction_reason and 'tags' == field:
                        redaction_reason = get_extra(package, 'redacted_tag_string', False)
                    if redaction_reason:
                        dataset[key] = '[[REDACTED-EX ' + redaction_reason + ']]'
                        continue

                if 'direct' == field_type and field:
                    if is_extra:
                        # log.debug('field: %s', field)
                        # log.debug('value: %s', get_extra(package, field))
                        dataset[key] = strip_if_string(get_extra(package, field, default))
                    else:
                        dataset[key] = strip_if_string(package.get(field, default))
                    if redaction_enabled and 'publisher' != field:
                        redaction_reason = get_extra(package, 'redacted_' + field, False)
                        # keywords(tags) have some UI-related issues with this, so we'll check both versions here
                        if redaction_reason:
                            dataset[key] = Package2Pod.mask_redacted(dataset[key], redaction_reason)
                            continue
                    else:
                        dataset[key] = Package2Pod.filter(dataset[key])

                elif 'array' == field_type:
                    if is_extra:
                        found_element = strip_if_string(get_extra(package, field))
                        if found_element:
                            if is_redacted(found_element):
                                dataset[key] = found_element
                            elif split:
                                dataset[key] = [Package2Pod.filter(x) for x in string.split(found_element, split)]

                    else:
                        if array_key:
                            dataset[key] = [Package2Pod.filter(t[array_key]) for t in package.get(field, {})]
                if wrapper:
                    # log.debug('wrapper: %s', wrapper)
                    method = getattr(Wrappers, wrapper)
                    if method:
                        Wrappers.current_field_map = field_map
                        dataset[key] = method(dataset.get(key))

            # CKAN doesn't like empty values on harvest, let's get rid of them
            # Remove entries where value is None, "", or empty list []
            dataset = OrderedDict([(x, y) for x, y in dataset.iteritems() if y is not None and y != "" and y != []])

            return dataset
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s : %s", exc_type, filename, exc_tb.tb_lineno, unicode(e))
            raise e

    @staticmethod
    def validate(pkg, dataset_dict):
        import sys, os

        global currentPackageOrg

        try:
            # When saved from UI DataQuality value is stored as "on" instead of True.
            # Check if value is "on" and replace it with True.
            dataset_dict = OrderedDict(dataset_dict)
            if dataset_dict.get('dataQuality') == "on" \
                    or dataset_dict.get('dataQuality') == "true" \
                    or dataset_dict.get('dataQuality') == "True":
                dataset_dict['dataQuality'] = True
            elif dataset_dict.get('dataQuality') == "false" \
                    or dataset_dict.get('dataQuality') == "False":
                dataset_dict['dataQuality'] = False

            errors = []
            try:
                from datajsonvalidator import do_validation
                do_validation([dict(dataset_dict)], errors, Package2Pod.seen_identifiers)
            except Exception as e:
                errors.append(("Internal Error", ["Something bad happened: " + unicode(e)]))
            if len(errors) > 0:
                for error in errors:
                    log.warn(error)

                try:
                    currentPackageOrg
                except NameError:
                    currentPackageOrg = 'unknown'

                errors_dict = OrderedDict([
                    ('id', pkg.get('id')),
                    ('name', Package2Pod.filter(pkg.get('name'))),
                    ('title', Package2Pod.filter(pkg.get('title'))),
                    ('organization', Package2Pod.filter(currentPackageOrg)),
                    ('errors', errors),
                ])

                return errors_dict

            return dataset_dict
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s", exc_type, filename, exc_tb.tb_lineno)
            raise e


class Wrappers:
    def __init__(self):
        pass

    redaction_enabled = False
    pkg = None
    current_field_map = None
    full_field_map = None
    bureau_code_list = None
    resource_formats = None

    @staticmethod
    def catalog_publisher(value):
        publisher = None
        if value:
            publisher = get_responsible_party(value)
        if not publisher and 'organization' in Wrappers.pkg and 'title' in Wrappers.pkg.get('organization'):
            publisher = Wrappers.pkg.get('organization').get('title')
        return OrderedDict([
            ("@type", "org:Organization"),
            ("name", publisher)
        ])

    @staticmethod
    def inventory_publisher(value):
        global currentPackageOrg

        publisher = strip_if_string(get_extra(Wrappers.pkg, Wrappers.current_field_map.get('field')))
        if publisher is None:
            return None

        currentPackageOrg = publisher

        organization_list = list()
        organization_list.append([
            ('@type', 'org:Organization'),  # optional
            ('name', Package2Pod.filter(publisher)),  # required
        ])

        for i in range(1, 6):
            pub_key = 'publisher_' + str(i)  # e.g. publisher_1
            if get_extra(Wrappers.pkg, pub_key):  # e.g. package.extras.publisher_1
                organization_list.append([
                    ('@type', 'org:Organization'),  # optional
                    ('name', Package2Pod.filter(get_extra(Wrappers.pkg, pub_key))),  # required
                ])
                currentPackageOrg = Package2Pod.filter(get_extra(Wrappers.pkg, pub_key))  # e.g. GSA

        if Wrappers.redaction_enabled:
            redaction_mask = get_extra(Wrappers.pkg, 'redacted_' + Wrappers.current_field_map.get('field'), False)
            if redaction_mask:
                return OrderedDict(
                    [
                        ('@type', 'org:Organization'),  # optional
                        ('name', '[[REDACTED-EX ' + redaction_mask + ']]'),  # required
                    ]
                )

        # so now we should have list() organization_list e.g.
        # (
        #   [('@type', 'org:Org'), ('name','GSA')],
        #   [('@type', 'org:Org'), ('name','OCSIT')]
        # )

        size = len(organization_list)  # e.g. 2

        tree = organization_list[0]
        for i in range(1, size):
            tree = organization_list[i] + [('subOrganizationOf', OrderedDict(tree))]

        return OrderedDict(tree)

    # used by get_accrual_periodicity
    accrual_periodicity_dict = {
        'completely irregular': 'irregular',
        'decennial': 'R/P10Y',
        'quadrennial': 'R/P4Y',
        'annual': 'R/P1Y',
        'bimonthly': 'R/P2M',  # or R/P0.5M
        'semiweekly': 'R/P3.5D',
        'daily': 'R/P1D',
        'biweekly': 'R/P2W',  # or R/P0.5W
        'semiannual': 'R/P6M',
        'biennial': 'R/P2Y',
        'triennial': 'R/P3Y',
        'three times a week': 'R/P0.33W',
        'three times a month': 'R/P0.33M',
        'continuously updated': 'R/PT1S',
        'monthly': 'R/P1M',
        'quarterly': 'R/P3M',
        'semimonthly': 'R/P0.5M',
        'three times a year': 'R/P4M',
        'weekly': 'R/P1W',
        'hourly': 'R/PT1H',
        'continual': 'R/PT1S',
        'fortnightly': 'R/P0.5M',
        'annually': 'R/P1Y',
        'biannualy': 'R/P0.5Y',
        'asneeded': 'irregular',
        'irregular': 'irregular',
        'notplanned': 'irregular',
        'unknown': 'irregular',
        'not updated': 'irregular'
    }

    @staticmethod
    def fix_accrual_periodicity(frequency):
        return Wrappers.accrual_periodicity_dict.get(str(frequency).lower().strip(), frequency)

    @staticmethod
    def build_contact_point(someValue):
        import sys, os

        try:
            contact_point_map = Wrappers.full_field_map.get('contactPoint').get('map')
            if not contact_point_map:
                return None

            package = Wrappers.pkg

            if contact_point_map.get('fn').get('extra'):
                fn = get_extra(package, contact_point_map.get('fn').get('field'),
                               get_extra(package, "Contact Name",
                                         package.get('maintainer')))
            else:
                fn = package.get(contact_point_map.get('fn').get('field'),
                                 get_extra(package, "Contact Name",
                                           package.get('maintainer')))

            fn = get_responsible_party(fn)

            if Wrappers.redaction_enabled:
                redaction_reason = get_extra(package, 'redacted_' + contact_point_map.get('fn').get('field'), False)
                if redaction_reason:
                    fn = Package2Pod.mask_redacted(fn, redaction_reason)
            else:
                fn = Package2Pod.filter(fn)

            if contact_point_map.get('hasEmail').get('extra'):
                email = get_extra(package, contact_point_map.get('hasEmail').get('field'),
                                  package.get('maintainer_email'))
            else:
                email = package.get(contact_point_map.get('hasEmail').get('field'),
                                    package.get('maintainer_email'))

            if email and not is_redacted(email) and '@' in email:
                email = 'mailto:' + email

            if Wrappers.redaction_enabled:
                redaction_reason = get_extra(package, 'redacted_' + contact_point_map.get('hasEmail').get('field'),
                                             False)
                if redaction_reason:
                    email = Package2Pod.mask_redacted(email, redaction_reason)
            else:
                email = Package2Pod.filter(email)

            contact_point = OrderedDict([('@type', 'vcard:Contact')])
            if fn:
                contact_point['fn'] = fn
            if email:
                contact_point['hasEmail'] = email

            return contact_point
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("%s : %s : %s", exc_type, filename, exc_tb.tb_lineno)
            raise e

    @staticmethod
    def inventory_parent_uid(parent_dataset_id):
        if parent_dataset_id:
            import ckan.model as model

            parent = model.Package.get(parent_dataset_id)
            parent_uid = parent.extras.col.target['unique_id'].value
            if parent_uid:
                parent_dataset_id = parent_uid
        return parent_dataset_id

    @staticmethod
    def generate_distribution(someValue):

        arr = []
        package = Wrappers.pkg

        distribution_map = Wrappers.full_field_map.get('distribution').get('map')
        if not distribution_map or 'resources' not in package:
            return arr

        for r in package["resources"]:
            resource = OrderedDict([('@type', "dcat:Distribution")])

            for pod_key, json_map in distribution_map.iteritems():
                value = strip_if_string(r.get(json_map.get('field'), json_map.get('default')))

                if Wrappers.redaction_enabled:
                    if 'redacted_' + json_map.get('field') in r and r.get('redacted_' + json_map.get('field')):
                        value = Package2Pod.mask_redacted(value, r.get('redacted_' + json_map.get('field')))
                else:
                    value = Package2Pod.filter(value)

                # filtering/wrapping if defined by export_map
                wrapper = json_map.get('wrapper')
                if wrapper:
                    method = getattr(Wrappers, wrapper)
                    if method:
                        value = method(value)

                if value:
                    resource[pod_key] = value

            # inventory rules
            res_url = strip_if_string(r.get('url'))
            if Wrappers.redaction_enabled:
                if 'redacted_url' in r and r.get('redacted_url'):
                    res_url = '[[REDACTED-EX ' + r.get('redacted_url') + ']]'
            else:
                res_url = Package2Pod.filter(res_url)

            if res_url:
                res_url = res_url.replace('http://[[REDACTED', '[[REDACTED')
                res_url = res_url.replace('http://http', 'http')
                if 'mediaType' in resource:
                    if 'accessURL' in resource:
                        resource.pop('accessURL')
                    resource['downloadURL'] = res_url
                else:
                    resource['accessURL'] = res_url
            else:
                log.warn("Missing downloadURL for resource in package ['%s']", package.get('id'))

            striped_resource = OrderedDict(
                [(x, y) for x, y in resource.iteritems() if y is not None and y != "" and y != []])

            arr += [OrderedDict(striped_resource)]

        # Add full metadata link to distribution, as described here:
        # https://github.com/GSA/ckanext-datajson/tree/main/ckanext/datajson/tests/datajson-samples#geospatial-full-metadata-link-example
        for e in package['extras']:
            if e['key'] == 'harvest_object_id':
                harvest_object_id = e['value']

                # Get full harvest object
                context = {'model': model, 'user': p.toolkit.c.user or p.toolkit.c.author}
                harvest_obj = logic.get_action('harvest_object_show')(context, {'id': harvest_object_id})

                # Build full metadata link
                resource = OrderedDict([('@type', "dcat:Distribution")])
                resource["title"] = "Original Metadata"
                resource["downloadURL"] = harvest_obj['extras']['waf_location']
                resource["conformsTo"] = "http://www.isotc211.org/2005/gmi"
                resource["description"] = "The metadata original source"
                resource["mediaType"] = "text/xml"
                resource["format"] = "XML"
                arr += [OrderedDict(resource)]

        return arr

    @staticmethod
    def bureau_code(value):
        if value:
            return value

        if 'organization' not in Wrappers.pkg:
            return None
        if 'title' not in Wrappers.pkg.get('organization'):
            return None
        org_title = Wrappers.pkg.get('organization').get('title')
        log.debug("org title: %s", org_title)

        code_list = Wrappers._get_bureau_code_list()

        if org_title not in code_list:
            default_bureau_code = Wrappers.full_field_map.get('bureauCode').get('default')
            if default_bureau_code is not None:
                log.debug("No match found, using default: %s", default_bureau_code)
                return [default_bureau_code]
            return None

        bureau = code_list.get(org_title)

        log.debug("found match: %s", "[{0}:{1}]".format(bureau.get('OMB Agency Code'), bureau.get('OMB Bureau Code')))
        result = "{0}:{1}".format(bureau.get('OMB Agency Code'), bureau.get('OMB Bureau Code'))
        log.debug("found match: '%s'", result)
        return [result]

    @staticmethod
    def _get_bureau_code_list():
        if Wrappers.bureau_code_list:
            return Wrappers.bureau_code_list
        import os
        bc_file = open(
            os.path.join(os.path.dirname(__file__), "resources", "omb-agency-bureau-treasury-codes.json"),
            "r"
        )
        code_list = json.load(bc_file)
        Wrappers.bureau_code_list = {}
        for bureau in code_list:
            Wrappers.bureau_code_list[bureau['Agency']] = bureau
        return Wrappers.bureau_code_list

    @staticmethod
    def mime_type_it(value):
        if not value:
            return value
        formats = h.resource_formats()
        format_clean = value.lower()
        if format_clean in formats:
            mime_type = formats[format_clean][0]
        else:
            mime_type = value
        msg = value + ' ... BECOMES ... ' + mime_type
        log.debug(msg)
        return mime_type

    @staticmethod
    def ckan_identifier(value):
        if value:
            return value
        return Wrappers.pkg.get('id')
        
    @staticmethod
    def modified_date(value):
        if value:
            return value
        return get_extra(Wrappers.pkg, 'metadata-date')
