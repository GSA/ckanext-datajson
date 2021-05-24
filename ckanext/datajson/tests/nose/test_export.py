from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
import json
import zipfile
from nose.tools import assert_in, assert_not_in
from io import StringIO
from nose.plugins.skip import SkipTest
import ckan.plugins as p

try:
    from ckan.tests.helpers import reset_db, FunctionalTestBase
    from ckan.tests import factories
    from ckan.common import config
except ImportError:  # CKAN 2.3
    from ckan.new_tests.helpers import reset_db, FunctionalTestBase
    from ckan.new_tests import factories
    from pylons import config


class TestExport(FunctionalTestBase):

    @classmethod
    def setup_class(cls):
        # Disable any extra plugins
        # Our rollup_extras patch conflicts with processing of
        # extras.publishing_status. Make sure not to load any other plugins to
        # avoid that.
        cls._ckan_plugins = config.get('ckan.plugins', None)
        config['ckan.plugins'] = 'datajson'

        # enable links (done in the INI file)
        cls._inventory_links_enabled = config.get('ckanext.datajson.inventory_links_enabled', None)
        config['ckanext.datajson.inventory_links_enabled'] = 'True'

        super(TestExport, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        reset_db()
        super(TestExport, cls).teardown_class()

        # Restore configuraiton
        if cls._ckan_plugins:
            config['ckan.plugins'] = cls._ckan_plugins
        if cls._inventory_links_enabled:
            config['ckanext.datajson.inventory_links_enabled'] = cls._inventory_links_enabled


    def create_datasets(self):

        self.user = factories.Sysadmin()
        self.user_name = self.user['name'].encode('ascii')
        self.organization = factories.Organization(
            name='myorg',
            users=[{'name': self.user_name, 'capacity': 'Admin'}]
            )
        
        dataset = {
            'public_access_level': 'public',
            'unique_id': '', 
            'contact_name': 'Jhon', 
            'program_code': '018:001', 
            'bureau_code': '019:20', 
            'contact_email': 'jhon@mail.com',
            'publisher': 'Publicher 01', 
            'modified': '2019-01-27 11:41:21', 
            'tag_string': 'tag01,tag02',
            'owner_org': self.organization['id'],
        }
        extra_for_draft = [{'key': 'publishing_status', 'value': 'Draft'}]
        d1 = dataset.copy()
        d1.update({'title': 'test 01 dataset', 'unique_id': 't1', 'extras': extra_for_draft})
        self.dataset1 = factories.Dataset(**d1)
        d2 = dataset.copy()
        d2.update({'title': 'test 02 dataset', 'unique_id': 't2'})
        self.dataset2 = factories.Dataset(**d2)
        d3 = dataset.copy()
        d3.update({'title': 'test 03 dataset', 'unique_id': 't3', 'extras': extra_for_draft})
        self.dataset3 = factories.Dataset(**d3)
        d4 = dataset.copy()
        d4.update({'title': 'test 04 dataset', 'unique_id': 't4'})
        self.dataset4 = factories.Dataset(**d4)

    def test_draft_json(self):
        """ test /org-id/draft.json """
        # create datasets
        self.create_datasets()

        self.app = self._get_test_app()
        url = '/organization/{}/draft.json'.format(self.organization['id'])
        extra_environ = {'REMOTE_USER': self.user_name}
        res = self.app.get(url, extra_environ=extra_environ)

        # zip file
        zip_path = '/tmp/test.zip'
        zf = open(zip_path, 'wb')
        zf.write(res.body)
        zf.close()
        zfile = zipfile.ZipFile(zip_path, 'r')
        for name in zfile.namelist():
            print('File Found {}'.format(name))
            zfile.extract(name)  # should create "draft_data.json"

        f = open('draft_data.json', 'r')
        raw = f.read()
        f.close()

        """
        Example data.json structure
        This is helpful to keep in mind while making assertions on the content.

        {
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema", 
            "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json", 
            "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld", 
            "@type": "dcat:Catalog", 
            "dataset": [
                {
                    "@type": "dcat:Dataset", 
                    "title": "resource upload testing & publishing 3", 
                    "description": "resource upload testing & publishing description", 
                    "modified": "2020-10-28T16:25:10.772Z", 
                    "accessLevel": "public", 
                    "identifier": "xxx-0-01", 
                    "license": "https://creativecommons.org/licenses/by/4.0/", 
                    "rights": "true", 
                    "publisher": 
                        {"@type": "org:Organization", "name": "BOP - Bureau of Prisons"}, 
                    "contactPoint": 
                        {"@type": "vcard:Contact", "fn": "joeq", "hasEmail": "mailto:joe@mail.com"}, 
                    "distribution": [
                        {"@type": "dcat:Distribution", "mediaType": "image/png", "title": "resource upload testing & publishing", "downloadURL": "https://inventory.sandbox.datagov.us/dataset/9fcc8e61-9f08-460a-a505-ca798076a9a9/resource/06b5522a-9abb-4f3e-a1ed-2d6ea5b0715f/download/2020-09-26.png"}
                        ], 
                    "keyword": ["tag"], 
                    "bureauCode": ["015:11"], 
                    "programCode": ["015:001"], 
                    "language": ["en-US"]
                    }
                ]
        }
        """

        data_res = json.loads(raw)
        datasets = data_res['dataset']
        titles = [d['title'] for d in datasets]

        assert_not_in(self.dataset2['title'], titles)
        assert_not_in(self.dataset4['title'], titles)

        assert_in(self.dataset1['title'], titles)
        assert_in(self.dataset3['title'], titles)
