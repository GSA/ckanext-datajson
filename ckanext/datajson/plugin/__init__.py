from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import ckan.plugins as p
import re


try:
    p.toolkit.requires_ckan_version("2.9")
except p.toolkit.CkanVersionException:
    from ckanext.datajson.plugin.pylons_plugin import MixinPlugin
else:
    from ckanext.datajson.plugin.flask_plugin import MixinPlugin


class DataJsonPlugin(MixinPlugin, p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurer)
    p.implements(p.ITemplateHelpers)

    def update_config(self, config):
        # Must use IConfigurer rather than IConfigurable because only IConfigurer
        # is called before after_map, in which we need the configuration directives
        # to know how to set the paths.

        # TODO commenting out enterprise data inventory for right now
        # self.route_edata_path = config.get("ckanext.enterprisedatajson.path", "/enterprisedata.json")
        self.route_enabled = p.toolkit.asbool(config.get("ckanext.datajson.url_enabled", True))
        self.route_path = config.get("ckanext.datajson.path", "/data.json")
        self.route_ld_path = config.get("ckanext.datajsonld.path", re.sub(r"\.json$", ".jsonld", self.route_path))
        self.ld_id = config.get("ckanext.datajsonld.id", config.get("ckan.site_url"))
        self.ld_title = config.get("ckan.site_title", "Catalog")
        self.site_url = config.get("ckan.site_url")
        self.inventory_links_enabled = p.toolkit.asbool(config.get("ckanext.datajson.inventory_links_enabled", False))

        # Adds our local templates directory. It's smart. It knows it's
        # relative to the path of *this* file. Wow.
        p.toolkit.add_template_directory(config, "../templates")

    def datajson_inventory_links_enabled(self):
        return self.inventory_links_enabled

    def get_helpers(self):
        return {
            'datajson_inventory_links_enabled': self.datajson_inventory_links_enabled
        }
