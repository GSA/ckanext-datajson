from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-datajson',
    version=version,
    description="CKAN extension to generate /data.json",
    long_description="""\
	""",
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='U.S. Department of Health & Human Services',
    author_email='',
    url='http://www.healthdata.gov',
    license='Public Domain',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.datajson'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points= \
        """
        [ckan.plugins]
        datajson=ckanext.datajson.plugin:DataJsonPlugin
        datajson_harvest=ckanext.datajson.plugin:DataJsonHarvester
        cmsdatanav_harvest=ckanext.datajson.plugin:CmsDataNavigatorHarvester
        """,
)
