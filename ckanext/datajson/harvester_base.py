from ckan import plugins as p


if p.toolkit.check_ckan_version(min_version='2.8.0'):
    from ckanext.datajson.datajson_ckan_28 import DatasetHarvesterBase
else:
    from ckanext.datajson.datajson_ckan_23 import DatasetHarvesterBase

__all__ = ["DatasetHarvesterBase"]