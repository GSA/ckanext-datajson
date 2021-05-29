# pylons controller
# TODO remove after CKAN 2.8 support is dropped
import json
import logging

import ckan.model as model
import ckan.plugins as p
from ckan.lib.base import BaseController, render, c
from ckan.plugins.toolkit import request, response

from ckanext.datagovtheme.export import DataJsonExporter


logger = logging.getLogger(__name__)


class DataJsonController(BaseController):
    def generate_json(self):
        return self.generate_output('json')

    def generate_org_json(self, org_id):
        return self.generate_output('json', org_id=org_id)

    # def generate_jsonld(self):
    #     return self.generate_output('json-ld')

    def generate_redacted(self, org_id):
        return self.generate('redacted', org_id=org_id)

    def generate_unredacted(self, org_id):
        return self.generate('unredacted', org_id=org_id)

    def generate_draft(self, org_id):
        return self.generate('draft', org_id=org_id)

    def generate(self, export_type='datajson', org_id=None):
        """ generate a JSON response """
        logger.debug('Generating JSON for {} to {} ({})'.format(export_type, org_id, c.user))

        if export_type not in ['draft', 'redacted', 'unredacted']:
            return "Invalid type"
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
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]
        return DataJsonExporter().make_json(export_type, org_id)

    def generate_output(self, fmt='json', org_id=None):
        # set content type (charset required or pylons throws an error)
        response.content_type = 'application/json; charset=UTF-8'

        # allow caching of response (e.g. by Apache)
        del response.headers["Cache-Control"]
        del response.headers["Pragma"]

        # TODO special processing for enterprise
        # output
        data = DataJsonExporter().make_json(export_type='datajson', owner_org=org_id)

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

    def validator(self):
        # Validates that a URL is a good data.json file.
        if request.method == "POST" and "url" in request.POST and request.POST["url"].strip() != "":
            c.source_url = request.POST["url"]
            c.errors = []

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
