from setuptools import setup, find_packages
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ckanext-datajson',
    version='0.1.15',
    description="CKAN extension to generate /data.json",
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3'
    ],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Data.gov',
    author_email='datagovhelp@gsa.gov',
    url='https://github.com/GSA/ckanext-datajson',
    license='Public Domain',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.datajson'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        'pyyaml',
        'jsonschema~=2.4.0',
        'rfc3987',
    ],
    setup_requires=['wheel'],
    entry_points="""
        [ckan.plugins]
    datajson=ckanext.datajson.plugin:DataJsonPlugin
    datajson_validator=ckanext.datajson.plugin:DataJsonValidatorPlugin
    datajson_harvest=ckanext.datajson.harvester_datajson:DataJsonHarvester
    cmsdatanav_harvest=ckanext.datajson.harvester_cmsdatanavigator:CmsDataNavigatorHarvester
    """,
)
