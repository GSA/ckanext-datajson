"""Unit tests for distribution mediaType handling during export.

Regression coverage for the data.json export error
``'mediaType' is a required property`` (``'required': ['mediaType']``).

The DCAT-US (POD v1.1) distribution schema requires ``mediaType`` whenever a
distribution has a ``downloadURL`` (pod_schema/federal-v1.1/dataset.json ->
``dependencies.downloadURL.required``). A "Link to a file" resource exports a
``downloadURL``; its ``mediaType`` is derived from the resource ``format``
field. When ``format`` is blank (e.g. the URL has no file extension for CKAN to
auto-detect), no ``mediaType`` was produced and the whole dataset was silently
dropped from the exported catalog.

These tests exercise ``Wrappers.generate_distribution`` directly so they need
no database, Solr, or running app.
"""

from jsonschema.validators import Draft4Validator

from ckanext.datajson.package2pod import Wrappers
from ckanext.datajson.helpers import get_validator


# The distribution portion of export.inventory.map.sample.json. This is the
# field map the inventory export uses; keeping a copy here makes the test
# independent of which export map happens to be configured.
INVENTORY_DISTRIBUTION_MAP = {
    "distribution": {
        "wrapper": "generate_distribution",
        "map": {
            "accessURL": {"field": "url"},
            "mediaType": {"field": "format", "wrapper": "mime_type_it"},
            "format": {"field": "formatReadable"},
            "title": {"field": "name"},
            "description": {"field": "description"},
        },
    }
}


def _generate_distribution(resources):
    """Run the real export wrapper against a package with the given resources."""
    Wrappers.redaction_enabled = False
    Wrappers.full_field_map = INVENTORY_DISTRIBUTION_MAP
    Wrappers.pkg = {"id": "pkg-test", "resources": resources}
    return Wrappers.generate_distribution(None)


# Subset of the POD v1.1 distribution definition: a downloadURL makes mediaType
# required. Used to assert the schema-level outcome without a full dataset.
DISTRIBUTION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "dependencies": {
        "downloadURL": {"required": ["mediaType"]},
    },
}


def _distribution_is_schema_valid(distribution):
    validator = Draft4Validator(DISTRIBUTION_SCHEMA)
    return not list(validator.iter_errors(distribution))


class TestDistributionMediaType(object):

    def test_file_resource_with_blank_format_still_has_media_type(self):
        """A downloadURL resource with no format must still get a mediaType."""
        dist = _generate_distribution([
            {"url": "https://example.com/download", "name": "r",
             "format": "", "resource_type": "file"},
        ])

        assert len(dist) == 1
        distribution = dist[0]
        assert "downloadURL" in distribution
        # The bug: mediaType was missing here, failing schema validation.
        assert "mediaType" in distribution
        assert _distribution_is_schema_valid(distribution)

    def test_blank_format_default_media_type_is_octet_stream(self):
        """When format can't yield a real MIME type, fall back to octet-stream."""
        dist = _generate_distribution([
            {"url": "https://example.com/download", "name": "r",
             "format": "", "resource_type": "file"},
        ])

        assert dist[0]["mediaType"] == "application/octet-stream"

    def test_known_format_media_type_is_preserved(self):
        """A real format must keep its proper MIME type, not the default."""
        dist = _generate_distribution([
            {"url": "https://example.com/data.csv", "name": "r",
             "format": "CSV", "resource_type": "file"},
        ])

        assert dist[0]["mediaType"] == "text/csv"
        assert _distribution_is_schema_valid(dist[0])

    def test_access_url_resource_has_no_media_type(self):
        """accessURL distributions must NOT carry a mediaType (and stay valid)."""
        dist = _generate_distribution([
            {"url": "https://example.com/api", "name": "r",
             "format": "", "resource_type": "accessurl"},
        ])

        assert "accessURL" in dist[0]
        assert "downloadURL" not in dist[0]
        assert "mediaType" not in dist[0]
        assert _distribution_is_schema_valid(dist[0])

    def test_exported_dataset_passes_full_pod_schema(self):
        """End-to-end: the blank-format file resource passes the real schema."""
        dist = _generate_distribution([
            {"url": "https://example.com/download", "name": "r",
             "format": "", "resource_type": "file"},
        ])

        dataset = {
            "@type": "dcat:Dataset",
            "title": "Test dataset",
            "description": "A description.",
            "identifier": "TEST-0001",
            "accessLevel": "public",
            "modified": "2020-01-01",
            "keyword": ["test"],
            "bureauCode": ["015:11"],
            "programCode": ["015:001"],
            "publisher": {"@type": "org:Organization", "name": "Test Org"},
            "contactPoint": {"@type": "vcard:Contact", "fn": "Jane",
                             "hasEmail": "mailto:jane@example.com"},
            "distribution": dist,
        }

        validator = get_validator()  # federal-v1.1 dataset.json
        errors = list(validator.iter_errors(dataset))
        assert errors == [], "Unexpected validation errors: %s" % (
            [e.message for e in errors])
