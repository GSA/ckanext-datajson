# flask blueprint

from flask import Blueprint


datajson = Blueprint("datajson", __name__)


@datajson.route('/pod/validate')
def datajsonvalidator():
    #m.connect('datajsonvalidator', "/pod/validate",
    #          controller='ckanext.datajson.plugin:DataJsonController', action='validator')
    pass

# Perhaps a better name is inventory mangement? inventory export?
inventory_links = Blueprint('inventory_links', __name__)


@inventory_links.route('/organization/<org_id>/redacted.json')
def public_data_listing(org_id):
    #m.connect('public_data_listing', '/organization/{org_id}/redacted.json',
    #          controller='ckanext.datajson.plugin:DataJsonController', action='generate_redacted')
    pass


@inventory_links.route('/organization/<org_id>/unredacted.json')
def enterprise_data_inventory(org_id):
    #m.connect('enterprise_data_inventory', '/organization/{org_id}/unredacted.json',
    #          controller='ckanext.datajson.plugin:DataJsonController', action='generate_unredacted')
    pass


@inventory_links.route('/organization/<org_id>/unredacted.json')
def enterprise_data_inventory_draft(org_id):
    #m.connect('enterprise_data_inventory', '/organization/{org_id}/draft.json',
    #          controller='ckanext.datajson.plugin:DataJsonController', action='generate_draft')
    pass


inventory_export = Blueprint('inventory_export', __name__)


@inventory_export.route('...')
def datajson_export():
    # /data.json and /data.jsonld (or other path as configured by user)
    #m.connect('datajson_export', self.route_path,
    #          controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')
    pass


@inventory_export.route('...')
def organization_export(org_id):
    #m.connect('organization_export', '/organization/{org_id}/data.json',
    #          controller='ckanext.datajson.plugin:DataJsonController', action='generate_org_json')
    pass
