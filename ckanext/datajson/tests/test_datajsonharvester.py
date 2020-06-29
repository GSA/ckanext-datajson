import copy
from urllib2 import URLError
from nose.tools import assert_equal, assert_raises, assert_in
import json
from mock import patch, MagicMock, Mock
from requests.exceptions import HTTPError, RequestException

try:
    from ckan.tests.helpers import reset_db, call_action
    from ckan.tests.factories import Organization, Group
except ImportError:
    from ckan.new_tests.helpers import reset_db, call_action
    from ckan.new_tests.factories import Organization, Group
from ckan import model
from ckan.plugins import toolkit
from ckan.lib.munge import munge_title_to_name
# from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
#                                              HarvestObjectObj)
from factories import (HarvestSourceObj,
                       HarvestJobObj,
                       HarvestObjectObj)

import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.datajson.harvester_datajson import DataJsonHarvester
import logging
log = logging.getLogger(__name__)

import mock_datajson_source


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

    def run_source(self, url):
        source = HarvestSourceObj(url=url)
        job = HarvestJobObj(source=source)

        harvester = DataJsonHarvester()

        # gather stage
        log.info('GATHERING %s', url)
        obj_ids = harvester.gather_stage(job)
        log.info('job.gather_errors=%s', job.gather_errors)
        log.info('obj_ids=%s', obj_ids)
        if len(obj_ids) == 0:
            # nothing to see
            return

        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        log.info('ho guid=%s', harvest_object.guid)
        log.info('ho content=%s', harvest_object.content)

        # fetch stage
        log.info('FETCHING %s', url)
        result = harvester.fetch_stage(harvest_object)

        log.info('ho errors=%s', harvest_object.errors)
        log.info('result 1=%s', result)

        # fetch stage
        log.info('IMPORTING %s', url)
        result = harvester.import_stage(harvest_object)

        log.info('ho errors 2=%s', harvest_object.errors)
        log.info('result 2=%s', result)
        log.info('ho pkg id=%s', harvest_object.package_id)
        dataset = model.Package.get(harvest_object.package_id)
        if dataset:
            log.info('dataset name=%s', dataset.name)
        errors = harvest_object.errors

        return harvest_object, result, dataset, errors

    def test_datason_arm(self):
        url = 'http://127.0.0.1:%s/arm' % mock_datajson_source.PORT
        harvest_object, result, dataset, errors = self.run_source(url=url)

        # assert_equal(first element on list
        expected_title = "NCEP GFS: vertical profiles of met quantities at standard pressures, at Barrow"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_in(munge_title_to_name("ORNL"), tags)
        assert_equal(len(dataset.resources), 1)
    
    def test_datason_usda(self):
        url = 'http://127.0.0.1:%s/usda' % mock_datajson_source.PORT
        harvest_object, result, dataset, errors = self.run_source(url=url)
        expected_title = "Pesticide Data Program"
        assert_equal(dataset.title, expected_title)
        tags = [tag.name for tag in dataset.get_tags()]
        assert_in("baby", tags)
        assert_equal(len(dataset.resources), 1)

    def test_datajson_reserverd_word_as_title(self):
        url = 'http://127.0.0.1:%s/error-reserved-title' % mock_datajson_source.PORT
        harvest_object, result, dataset, errors = self.run_source(url=url)
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "title: Search. That name cannot be used."
        assert_equal(errors[0].message, expected_error_message)
    
    def test_datajson_large_spatial(self):
        url = 'http://127.0.0.1:%s/error-large-spatial' % mock_datajson_source.PORT
        harvest_object, result, dataset, errors = self.run_source(url=url)
        expected_error_stage = "Import"
        assert_equal(errors[0].stage, expected_error_stage)
        expected_error_message = "spatial: Maximum allowed size is 32766. Actual size is 309643."
        assert_equal(errors[0].message, expected_error_message)

    def test_datajson_null_spatial(self):
        url = 'http://127.0.0.1:%s/null-spatial' % mock_datajson_source.PORT
        harvest_object, result, dataset, errors = self.run_source(url=url)
        expected_title = "Sample Title NUll Spatial"
        assert_equal(dataset.title, expected_title)
   
    def test_datason_404(self):
        url = 'http://127.0.0.1:%s/404' % mock_datajson_source.PORT
        with assert_raises(URLError) as harvest_context:
            self.run_source(url=url)
        
    def test_datason_500(self):
        url = 'http://127.0.0.1:%s/500' % mock_datajson_source.PORT
        with assert_raises(URLError) as harvest_context:
            self.run_source(url=url)
