from datetime import datetime
import json
import logging
from urllib2 import URLError

import ckan.plugins as p
import ckanext.harvest.model as harvest_model
import ckanext.harvest.queue as queue
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

from nose.plugins.skip import SkipTest

class TestDataJSONHarvester23(object):

    @classmethod
    def setup_class(cls):
        log.info('Starting mock http server')
        cls.mock_port = 8960
        mock_datajson_source.serve(cls.mock_port)

    @classmethod
    def setup(cls):
        # Start data json sources server we can test harvesting against it
        reset_db()
        harvest_model.setup()
        cls.user = Sysadmin()

        if p.toolkit.check_ckan_version(min_version='2.8.0'):
            raise SkipTest('Just for CKAN 2.3')
        
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

    def run_import(self, objects=None):
        # import stage
        datasets = []
        
        # allow run just some objects
        if objects is None:
            # default is all objects in the right order
            objects = self.harvest_objects
        else:
            log.info('Import custom list {}'.format(objects))
        
        for harvest_object in objects:
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

    def test_datajson_collection(self):
        """ harvest from a source with a parent in the second place
            We expect the gather stage to re-order to the forst place """
        url = 'http://127.0.0.1:%s/collection-1-parent-2-children.data.json' % self.mock_port
        obj_ids = self.run_gather(url=url)

        identifiers = []
        for obj_id in obj_ids:
            harvest_object = harvest_model.HarvestObject.get(obj_id)
            content = json.loads(harvest_object.content)
            identifiers.append(content['identifier'])

        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        expected_obj_ids = ['OPM-ERround-0001']
        
        assert_equal(expected_obj_ids, identifiers)
    
    def test_harvesting_parent_child_collections(self):
        """ Test that parent are beeing harvested first.
            When we harvest a child the parent must exists
            data.json from: https://www.opm.gov/data.json """

        url = 'http://127.0.0.1:%s/collection-1-parent-2-children.data.json' % self.mock_port
        obj_ids = self.run_gather(url=url)

        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(len(obj_ids), 1)
        
        self.run_fetch()
        datasets = self.run_import()

        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(len(datasets), 1)
        titles = ['Employee Relations Roundtables']
        
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

        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(child_counter, 0)

        assert_equal(parent_counter, 1)
    
    def get_datasets_from_2_collection(self):
        url = 'http://127.0.0.1:%s/collection-2-parent-4-children.data.json' % self.mock_port
        obj_ids = self.run_gather(url=url)

        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(len(obj_ids), 2)
        
        self.run_fetch()
        datasets = self.run_import()
        
        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(len(datasets), 2)
        
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
        
        assert_equal(len(jobs), 2)
        # Old harvester go from parents_run to children_run (a second job for children)
        assert_equal(source_config.get('datajson_collection'), 'children_run')
        
        return datasets

    def test_datasets_count(self):
        """ test we harvest the right amount of datasets """

        datasets = self.get_datasets_from_2_collection()
        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(len(datasets), 2)
        
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
        # at CKAN 2.3 with GSA ckanext-harvest fork we expect just parents
        # after "parents_run" a new job will be raised for children
        assert_equal(child_counter, 0)
