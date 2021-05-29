"""
Mixin for Pylons-specific functionality. This aides the migration between Pylons and Flask.
"""


import ckan.plugins as p


class MixinPlugin(object):
    p.implements(p.interfaces.IRoutes, inherit=True)

    def before_map(self, m):
        return m

    def after_map(self, m):
        if self.route_enabled:
            # /data.json and /data.jsonld (or other path as configured by user)
            m.connect('datajson_export', self.route_path,
                      controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_json')
            m.connect('organization_export', '/organization/{org_id}/data.json',
                      controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_org_json')
            # TODO commenting out enterprise data inventory for right now
            # m.connect('enterprisedatajson', self.route_edata_path,
            # controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_enterprise')

            # m.connect('datajsonld', self.route_ld_path,
            # controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_jsonld')

        if self.inventory_links_enabled:
            m.connect('public_data_listing', '/organization/{org_id}/redacted.json',
                      controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_redacted')

            m.connect('enterprise_data_inventory', '/organization/{org_id}/unredacted.json',
                      controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_unredacted')

            m.connect('enterprise_data_inventory', '/organization/{org_id}/draft.json',
                      controller='ckanext.datajson.controllers.view:DataJsonController', action='generate_draft')

        # /pod/validate
        m.connect('datajsonvalidator', "/pod/validate",
                  controller='ckanext.datajson.controllers.view:DataJsonController', action='validator')

        return m
