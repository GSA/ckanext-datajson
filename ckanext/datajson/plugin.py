from __future__ import absolute_import

from future import standard_library

standard_library.install_aliases()

import re

import ckan.plugins as p

from . import blueprint

# try:
#     from collections import OrderedDict  # 2.7
# except ImportError:
#     from sqlalchemy.util import OrderedDict


class DataJsonPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.ITemplateHelpers)
    p.implements(p.interfaces.IRoutes, inherit=True)
    p.implements(p.IBlueprint)

    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.

        DataJsonPlugin.route_enabled = config.get("ckanext.datajson.url_enabled", "True") == 'True'
        DataJsonPlugin.route_path = config.get("ckanext.datajson.path", "/internal/data.json")
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

    # def after_map(self, m):
    #     if DataJsonPlugin.route_enabled:
    #         # /data.json and /data.jsonld (or other path as configured by user)
    #         m.connect('datajson_export', DataJsonPlugin.route_path,
    #                   controller='ckanext.datajson.plugin:DataJsonController', action='generate_json')
    #         m.connect('organization_export', '/organization/{org_id}/data.json',
    #                   controller='ckanext.datajson.plugin:DataJsonController', action='generate_org_json')
    #         # TODO commenting out enterprise data inventory for right now
    #         # m.connect('enterprisedatajson', DataJsonPlugin.route_edata_path,
    #         # controller='ckanext.datajson.plugin:DataJsonController', action='generate_enterprise')

    #         # m.connect('datajsonld', DataJsonPlugin.route_ld_path,
    #         # controller='ckanext.datajson.plugin:DataJsonController', action='generate_jsonld')

    #     if DataJsonPlugin.inventory_links_enabled:
    #         m.connect('public_data_listing', '/organization/{org_id}/redacted.json',
    #                   controller='ckanext.datajson.plugin:DataJsonController', action='generate_redacted')

    #         m.connect('enterprise_data_inventory', '/organization/{org_id}/unredacted.json',
    #                   controller='ckanext.datajson.plugin:DataJsonController', action='generate_unredacted')

    #         m.connect('enterprise_data_inventory', '/organization/{org_id}/draft.json',
    #                   controller='ckanext.datajson.plugin:DataJsonController', action='generate_draft')

    #     # /pod/validate
    #     m.connect('datajsonvalidator', "/pod/validate",
    #               controller='ckanext.datajson.plugin:DataJsonController', action='validator')

    #     return m
    
    

    def get_blueprint(self):
        return blueprint.datapusher
