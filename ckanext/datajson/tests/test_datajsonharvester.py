import json
import logging
from urllib2 import URLError

import ckanext.harvest.model as harvest_model
import mock_datajson_source
from ckan import model
from ckan.lib.munge import munge_title_to_name
from ckanext.datajson.harvester_datajson import DataJsonHarvester

from factories import HarvestJobObj, HarvestSourceObj
from nose.tools import (assert_equal, assert_false, assert_in, assert_raises,
                        assert_true)

try:
    from ckan.tests.helpers import reset_db, call_action
    from ckan.tests.factories import Organization, Group
except ImportError:
    from ckan.new_tests.helpers import reset_db, call_action
    from ckan.new_tests.factories import Organization, Group

log = logging.getLogger(__name__)


class TestDataJSONHarvester(object):

    @classmethod
    def setup_class(cls):
        log.info('Starting mock http server')
        mock_datajson_source.serve()

    @classmethod
    def setup(cls):
        # Start data json sources server we can test harvesting against it
        reset_db()
        harvest_model.setup()

    def run_gather(self, url):
        source = HarvestSourceObj(url=url)
        job = HarvestJobObj(source=source)

        self.harvester = DataJsonHarvester()

        # gather stage
        log.info('GATHERING %s', url)
        obj_ids = self.harvester.gather_stage(job)
        log.info('job.gather_errors=%s', job.gather_errors)
        log.info('obj_ids=%s', obj_ids)
        if len(obj_ids) == 0:
            # nothing to see
            return

        self.harvest_objects = []
        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            log.info('ho guid=%s', harvest_object.guid)
            log.info('ho content=%s', harvest_object.content)
            self.harvest_objects.append(harvest_object)

        return obj_ids

    def run_fetch(self):
        # fetch stage

        for harvest_object in self.harvest_objects:
            log.info('FETCHING %s' % harvest_object.id)
            result = self.harvester.fetch_stage(harvest_object)

            log.info('ho errors=%s', harvest_object.errors)
            log.info('result 1=%s', result)
            if len(harvest_object.errors) > 0:
                self.errors = harvest_object.errors

    def run_import(self):
        # import stage
        datasets = []
        for harvest_object in self.harvest_objects:
            log.info('IMPORTING %s' % harvest_object.id)
            result = self.harvester.import_stage(harvest_object)
            
            log.info('ho errors 2=%s', harvest_object.errors)
            log.info('result 2=%s', result)
            
            if not result:
                log.error('Dataset not imported: {}. Errors: {}. Content: {}'.format(harvest_object.package_id, harvest_object.errors, harvest_object.content))

            if len(harvest_object.errors) > 0:
                self.errors = harvest_object.errors

            log.info('ho pkg id=%s', harvest_object.package_id)
            dataset = model.Package.get(harvest_object.package_id)
            if dataset:
                datasets.append(dataset)
                log.info('dataset name=%s', dataset.name)

        return datasets

    def run_source(self, url):
        self.run_gather(url)
        self.run_fetch()
        datasets = self.run_import()

        return datasets

    def test_datason_arm(self):
        url = 'http://127.0.0.1:%s/arm' % mock_datajson_source.PORT
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        # assert_equal(first element on list
        expected_title = "NCEP GFS: vertical profiles of met quantities at standard pressures, at Barrow"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_in(munge_title_to_name("ORNL"), tags)
        assert_equal(len(dataset.resources), 1)

    def test_datason_usda(self):
        url = 'http://127.0.0.1:%s/usda' % mock_datajson_source.PORT
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        expected_title = "Department of Agriculture Congressional Logs for Fiscal Year 2014"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_equal(len(dataset.resources), 1)
        assert_in(munge_title_to_name("Congressional Logs"), tags)

    def test_datajson_collection(self):
        """ harvest from a source with a parent in the second place
            We expect the gather stage to re-order to the forst place """
        url = 'http://127.0.0.1:%s/collections' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)

        identifiers = []
        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            content = json.loads(harvest_object.content)
            identifiers.append(content['identifier'])

        # We always expect the parent to be the first on the list
        expected_obj_ids = ['OPM-ERround-0001', 'OPM-ERround-0001-AWOL', 'OPM-ERround-0001-Retire']
        assert_equal(expected_obj_ids, identifiers)
    
    def test_harvesting_parent_child_collections(self):
        """ Test that parent are beeing harvested first.
            When we harvest a child the parent must exists
            data.json from: https://www.opm.gov/data.json """

        url = 'http://127.0.0.1:%s/collections' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)
        assert_equal(len(obj_ids), 3)
        self.run_fetch()
        datasets = self.run_import()
        assert_equal(len(datasets), 3)
        titles = ['Linking Employee Relations and Retirement',
                  'Addressing AWOL',
                  'Employee Relations Roundtables']

        parent_counter = 0
        child_counter = 0
        
        for dataset in datasets:
            assert dataset.title in titles
            # test we get the spatial as we want: https://github.com/GSA/catalog.data.gov/issues/55
            extras = json.loads(dataset.extras['extras_rollup'])
            is_parent = extras.get('collection_metadata', 'false').lower() == 'true'
            is_child = extras.get('collection_package_id', None) is not None

            log.info('Harvested dataset {} {} {}'.format(dataset.title, is_parent, is_child))

            if dataset.title in ['Employee Relations Roundtables']:
                assert_equal(is_parent, True)
                parent_counter += 1
            else:
                assert_equal(is_child, True)
                child_counter += 1

        assert_equal(child_counter, 2)
        assert_equal(parent_counter, 1)
    
    def test_harvesting_parent_child_2_collections(self):
        """ Test that parent are beeing harvested first.
            When we harvest a child the parent must exists
            data.json from: https://www.opm.gov/data.json """

        url = 'http://127.0.0.1:%s/collections2' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)
        assert_equal(len(obj_ids), 6)
        self.run_fetch()
        datasets = self.run_import()
        assert_equal(len(datasets), 6)
        titles = ['Linking Employee Relations and Retirement',
                  'Addressing AWOL',
                  'Employee Relations Roundtables',
                  'Linking Employee Relations and Retirement 2',
                  'Addressing AWOL 2',
                  'Employee Relations Roundtables 2']

        parent_counter = 0
        child_counter = 0
        
        for dataset in datasets:
            assert dataset.title in titles
            # test we get the spatial as we want: https://github.com/GSA/catalog.data.gov/issues/55
            extras = json.loads(dataset.extras['extras_rollup'])
            is_parent = extras.get('collection_metadata', 'false').lower() == 'true'
            is_child = extras.get('collection_package_id', None) is not None

            log.info('Harvested dataset {} {} {}'.format(dataset.title, is_parent, is_child))

            if dataset.title in ['Employee Relations Roundtables', 'Employee Relations Roundtables 2']:
                assert_equal(is_parent, True)
                parent_counter += 1
            else:
                assert_equal(is_child, True)
                child_counter += 1

        assert_equal(child_counter, 4)
        assert_equal(parent_counter, 2)

    def test_datajson_is_part_of_package_id(self):
        url = 'http://127.0.0.1:%s/collections' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)

        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            content = json.loads(harvest_object.content)
            results = self.harvester.is_part_of_to_package_id(content['identifier'], harvest_object)
            if content['identifier'] == 'OPM-ERround-0001':
                assert_false(results)
            if content['identifier'] == 'OPM-ERround-0001-AWOL':
                assert_true(results)
            if content['identifier'] == 'OPM-ERround-0001-Retire':
                assert_true(results)

        results = self.harvester.is_part_of_to_package_id('bad identifier', harvest_object)
        assert_equal(results, False)

    def test_datajson_reserverd_word_as_title(self):
        url = 'http://127.0.0.1:%s/error-reserved-title' % mock_datajson_source.PORT
        self.run_source(url=url)
        errors = self.errors
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "title: Search. That name cannot be used."
        assert_equal(errors[0].message, expected_error_message)

    def test_datajson_large_spatial(self):
        url = 'http://127.0.0.1:%s/error-large-spatial' % mock_datajson_source.PORT
        self.run_source(url=url)
        errors = self.errors
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "spatial: Maximum allowed size is 32766. Actual size is 309643."
        assert_equal(errors[0].message, expected_error_message)

    def test_datajson_null_spatial(self):
        url = 'http://127.0.0.1:%s/null-spatial' % mock_datajson_source.PORT
        datasets = self.run_source(url=url)
        dataset = datasets[0]
        expected_title = "Sample Title NUll Spatial"
        assert_equal(dataset.title, expected_title)

    def test_datason_404(self):
        url = 'http://127.0.0.1:%s/404' % mock_datajson_source.PORT
        with assert_raises(URLError):
            self.run_source(url=url)

    def test_datason_500(self):
        url = 'http://127.0.0.1:%s/500' % mock_datajson_source.PORT
        with assert_raises(URLError):
            self.run_source(url=url)
