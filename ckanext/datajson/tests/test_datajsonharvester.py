from datetime import datetime
import json
import logging
from urllib2 import URLError

import ckan.plugins as p
import ckanext.harvest.model as harvest_model
import mock_datajson_source
from ckan import model
from ckan.lib.munge import munge_title_to_name
from ckanext.datajson.harvester_datajson import DataJsonHarvester
from ckanext.datajson.exceptions import ParentNotHarvestedException
from factories import HarvestJobObj, HarvestSourceObj
from mock import Mock, patch
from nose.tools import (assert_equal, assert_false, assert_in, assert_is_none,
                        assert_raises, assert_true)

try:
    from ckan.tests.helpers import reset_db, call_action
    from ckan.tests.factories import Organization, Group, Sysadmin
except ImportError:
    from ckan.new_tests.helpers import reset_db, call_action
    from ckan.new_tests.factories import Organization, Group, Sysadmin

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
        cls.user = Sysadmin()
        
    def run_gather(self, url):
        self.source = HarvestSourceObj(url=url)
        self.job = HarvestJobObj(source=self.source)

        self.harvester = DataJsonHarvester()

        # gather stage
        log.info('GATHERING %s', url)
        obj_ids = self.harvester.gather_stage(self.job)
        log.info('job.gather_errors=%s', self.job.gather_errors)
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
                harvest_object.state = "ERROR"
            
            harvest_object.state = "COMPLETE"
            harvest_object.save()

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
        url = 'http://127.0.0.1:%s/collection-1-parent-2-children.data.json' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)

        identifiers = []
        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            content = json.loads(harvest_object.content)
            identifiers.append(content['identifier'])

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            expected_obj_ids = ['OPM-ERround-0001']
        else:
            # We always expect the parent to be the first on the list
            expected_obj_ids = ['OPM-ERround-0001', 'OPM-ERround-0001-AWOL', 'OPM-ERround-0001-Retire']
        assert_equal(expected_obj_ids, identifiers)
    
    def test_harvesting_parent_child_collections(self):
        """ Test that parent are beeing harvested first.
            When we harvest a child the parent must exists
            data.json from: https://www.opm.gov/data.json """

        url = 'http://127.0.0.1:%s/collection-1-parent-2-children.data.json' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(len(obj_ids), 1)
        else:
            assert_equal(len(obj_ids), 3)

        self.run_fetch()
        datasets = self.run_import()

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(len(datasets), 1)
            titles = ['Employee Relations Roundtables']
        else:
            assert_equal(len(datasets), 3)
            titles = ['Linking Employee Relations and Retirement',
                    'Addressing AWOL',
                    'Employee Relations Roundtables']

        parent_counter = 0
        child_counter = 0
        
        for dataset in datasets:
            assert dataset.title in titles
            
            is_parent = dataset.extras.get('collection_metadata', 'false').lower() == 'true'
            is_child = dataset.extras.get('collection_package_id', None) is not None

            log.info('Harvested dataset {} {} {}'.format(dataset.title, is_parent, is_child))

            if dataset.title == 'Employee Relations Roundtables':
                assert_equal(is_parent, True)
                assert_equal(is_child, False)
                parent_counter += 1
            else:
                assert_equal(is_child, True)
                assert_equal(is_parent, False)
                child_counter += 1

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(child_counter, 0)
        else:
            assert_equal(child_counter, 2)
        assert_equal(parent_counter, 1)
    
    def get_datasets_from_2_collection(self):
        url = 'http://127.0.0.1:%s/collection-2-parent-4-children.data.json' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(len(obj_ids), 2)
        else:
            assert_equal(len(obj_ids), 6)

        self.run_fetch()
        datasets = self.run_import()
        
        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(len(datasets), 2)
        else:
            assert_equal(len(datasets), 6)
        
        return datasets

    @patch('ckanext.harvest.logic.action.update.harvest_source_show')
    def test_new_job_created(self, mock_harvest_source_show):
        """ with CKAN 2.3 we divide the harvest job for collection in two steps:
            (one for parents and a second one for children).
            After finish tha parent job a new job is created for children
            """
        def ps(context, data):
            return {
                    u'id': self.source.id,
                    u'title': self.source.title, 
                    u'state': u'active',
                    u'type': u'harvest', 
                    u'source_type': self.source.type, 
                    u'active': False,
                    u'name': u'test_source_0',
                    u'url': self.source.url,
                    u'extras': []
                }

        # just for CKAN 2.3
        mock_harvest_source_show.side_effect = ps

        datasets = self.get_datasets_from_2_collection()
        
        # in CKAN 2.3 we expect a new job for this source and also a change in the source config 
        
        context = {'model': model, 'user': self.user['name'], 'session':model.Session}

        # fake job status before final RUN command.
        self.job.status = u'Running'
        self.job.gather_finished = datetime.utcnow()
        self.job.save()

        # mark finished and do the after job tasks (in CKAN 2.3 is to create a new job for children)
        p.toolkit.get_action('harvest_jobs_run')(context, {'source_id': self.source.id})
        
        jobs = harvest_model.HarvestJob.filter(source=self.source).all()
        source_config = json.loads(self.source.config or '{}')
        
        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            assert_equal(len(jobs), 2)
            # Old harvester go from parents_run to children_run (a second job for children)
            assert_equal(source_config.get('datajson_collection'), 'children_run')
        else:
            assert_equal(len(jobs), 1)
            # New harvester never go from parents_run to children_run
            assert_equal(source_config.get('datajson_collection', ''), 'parents_run')
            assert_equal(jobs[0].status, 'Finished')

        return datasets

    def test_datasets_count(self):
        """ test we harvest the right amount of datasets """

        datasets = self.get_datasets_from_2_collection()
        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(len(datasets), 2)
        else:
            assert_equal(len(datasets), 6)
    
    def test_parent_child_counts(self):
        """ Test count for parent and children """
        
        datasets = self.get_datasets_from_2_collection()
        
        parent_counter = 0
        child_counter = 0
        
        for dataset in datasets:
            
            is_parent = dataset.extras.get('collection_metadata', 'false').lower() == 'true'
            parent_package_id = dataset.extras.get('collection_package_id', None)
            is_child = parent_package_id is not None

            if is_parent:
                parent_counter += 1
            elif is_child:
                child_counter += 1

        assert_equal(parent_counter, 2)

        if p.toolkit.check_ckan_version(max_version='2.7.99'):
            # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
            # after "parents_run" a new job will be raised for children
            assert_equal(child_counter, 0)
        else:
            assert_equal(child_counter, 4)

    def test_harvesting_parent_child_2_collections(self):
        """ Test that we have the right parents in each case """
        
        datasets = self.get_datasets_from_2_collection()
        
        for dataset in datasets:
            parent_package_id = dataset.extras.get('collection_package_id', None)
            
            if dataset.title == 'Addressing AWOL':
                parent = model.Package.get(parent_package_id)
                # HEREX parent is None
                assert_equal(parent.title, 'Employee Relations Roundtables')
            elif dataset.title == 'Addressing AWOL 2':
                parent = model.Package.get(parent_package_id)
                assert_equal(parent.title, 'Employee Relations Roundtables 2') 

    def test_datajson_is_part_of_package_id(self):
        url = 'http://127.0.0.1:%s/collection-1-parent-2-children.data.json' % mock_datajson_source.PORT
        obj_ids = self.run_gather(url=url)
        self.run_fetch()
        self.run_import()

        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            content = json.loads(harvest_object.content)
            # get the dataset with this identifier only if is a parent in a collection
            if content['identifier'] == 'OPM-ERround-0001':
                dataset = self.harvester.is_part_of_to_package_id(content['identifier'], harvest_object)
                assert_equal(dataset['title'], 'Employee Relations Roundtables')

            if content['identifier'] in ['OPM-ERround-0001-AWOL', 'OPM-ERround-0001-Retire']:
                with assert_raises(ParentNotHarvestedException):
                    self.harvester.is_part_of_to_package_id(content['identifier'], harvest_object)
            
        with assert_raises(ParentNotHarvestedException):
            self.harvester.is_part_of_to_package_id('bad identifier', harvest_object)
        
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

    @patch('ckan.plugins.toolkit.get_action')
    def test_is_part_of_to_package_id_fail_no_results(self, mock_get_action):
        """ unit test for is_part_of_to_package_id function """

        def get_action(action_name):
            # CKAN 2.8 have the "mock_action" decorator but this is not available for CKAN 2.3
            if action_name == 'package_search':
                return lambda ctx, data: {'count': 0}
            elif action_name == 'get_site_user':
                return lambda ctx, data: {'name': 'default'}

        mock_get_action.side_effect = get_action
        
        harvester = DataJsonHarvester()
        with assert_raises(ParentNotHarvestedException):
            harvester.is_part_of_to_package_id('identifier', None)
        
    @patch('ckanext.datajson.harvester_datajson.DataJsonHarvester.get_harvest_source_id')
    @patch('ckan.plugins.toolkit.get_action')
    def test_is_part_of_to_package_id_one_result(self, mock_get_action, mock_get_harvest_source_id):
        """ unit test for is_part_of_to_package_id function """
        
        results = {
            'count': 1, 
            'results': [
                {'id': 'pkg-1', 
                 'name': 'dataset-1', 
                 'extras': [{'key': 'identifier', 'value': 'identifier'}]}
                ]}
        def get_action(action_name):
            # CKAN 2.8 have the "mock_action" decorator but this is not available for CKAN 2.3
            if action_name == 'package_search':
                return lambda ctx, data: results
            elif action_name == 'get_site_user':
                return lambda ctx, data: {'name': 'default'}

        mock_get_action.side_effect = get_action
        mock_get_harvest_source_id.side_effect = lambda package_id: 'hsi-{}'.format(package_id)
        
        harvest_source = Mock()
        harvest_source.id = 'hsi-pkg-1'
        harvest_object = Mock()
        harvest_object.source = harvest_source

        harvester = DataJsonHarvester()
        dataset = harvester.is_part_of_to_package_id('identifier', harvest_object)
        assert mock_get_action.called
        assert_equal(dataset['name'], 'dataset-1')
    
    @patch('ckanext.datajson.harvester_datajson.DataJsonHarvester.get_harvest_source_id')
    @patch('ckan.plugins.toolkit.get_action')
    def test_is_part_of_to_package_id_two_result(self, mock_get_action, mock_get_harvest_source_id):
        """ unit test for is_part_of_to_package_id function 
            Test for 2 parents with the same identifier. 
            Just one belongs to the right harvest source """
        
        results = {
            'count': 2,
            'results':[
            {'id': 'pkg-1',
             'name': 'dataset-1', 
             'extras': [{'key': 'identifier', 'value': 'custom-identifier'}]},
            {'id': 'pkg-2',
             'name': 'dataset-2',
             'extras': [{'key': 'identifier', 'value': 'custom-identifier'}]}
            ]}
            
        def get_action(action_name):
            # CKAN 2.8 have the "mock_action" decorator but this is not available for CKAN 2.3
            if action_name == 'package_search':
                return lambda ctx, data: results
            elif action_name == 'get_site_user':
                return lambda ctx, data: {'name': 'default'}

        mock_get_action.side_effect = get_action
        mock_get_harvest_source_id.side_effect = lambda package_id: 'hsi-{}'.format(package_id)

        harvest_source = Mock()
        harvest_source.id = 'hsi-pkg-2'
        harvest_object = Mock()
        harvest_object.source = harvest_source
        
        harvester = DataJsonHarvester()
        dataset = harvester.is_part_of_to_package_id('custom-identifier', harvest_object)
        assert mock_get_action.called
        assert_equal(dataset['name'], 'dataset-2')

    @patch('ckanext.datajson.harvester_datajson.DataJsonHarvester.get_harvest_source_id')
    @patch('ckan.plugins.toolkit.get_action')
    def test_parent_not_harvested_exception(self, mock_get_action, mock_get_harvest_source_id):
        """ unit test for is_part_of_to_package_id function 
            Test for 2 parents with the same identifier. 
            Just one belongs to the right harvest source """
        
        results = {
            'count': 2,
            'results':[
            {'id': 'pkg-1',
             'name': 'dataset-1', 
             'extras': [{'key': 'identifier', 'value': 'custom-identifier'}]},
            {'id': 'pkg-2',
             'name': 'dataset-2',
             'extras': [{'key': 'identifier', 'value': 'custom-identifier'}]}
            ]}
            
        def get_action(action_name):
            # CKAN 2.8 have the "mock_action" decorator but this is not available for CKAN 2.3
            if action_name == 'package_search':
                return lambda ctx, data: results
            elif action_name == 'get_site_user':
                return lambda ctx, data: {'name': 'default'}

        mock_get_action.side_effect = get_action
        mock_get_harvest_source_id.side_effect = lambda package_id: 'hsi-{}'.format(package_id)

        harvest_source = Mock()
        harvest_source.id = 'hsi-pkg-99'  # raise error, not found
        harvest_object = Mock()
        harvest_object.source = harvest_source
        
        harvester = DataJsonHarvester()
        with assert_raises(ParentNotHarvestedException):
            harvester.is_part_of_to_package_id('custom-identifier', harvest_object)
        
        assert mock_get_action.called
        

        
