"""
Mixin for Flask-specific functionality. This aides the migration between Pylons and Flask.
"""

import ckan.plugins as p

from ckanext.datajson import views


class MixinPlugin(object):
    p.implements(p.IBlueprint)

    # IBlueprint
    def get_blueprint(self):
        blueprints = [views.datajson]

        if self.route_enabled:
            blueprints += [views.inventory_export]

        if self.inventory_links_enabled:
            blueprints += [views.inventory_links]

        return blueprints
