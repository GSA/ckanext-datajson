try:
    from collections import OrderedDict # 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

def dataset_to_jsonld(dataset):
    from plugin import DataJsonPlugin
	
    ret = OrderedDict([
       ("@id", DataJsonPlugin.site_url + "/dataset/" + dataset["identifier"]),
       ("@type", "dcat:Dataset"),
    ])
    
    apply_jsonld_metadata_mapping(dataset, ret)
    
    for d in dataset.get("distribution", []):
        dd = distribution_to_jsonld(d)
        ret.setdefault("dcat:distribution", []).append(dd)
        
    return ret
        
def distribution_to_jsonld(distribution):
    from plugin import DataJsonPlugin
    ret = OrderedDict([
       ("@id", DataJsonPlugin.site_url + "/resource/" + distribution["identifier"]),
       ("@type", "dcat:Distribution"),
    ])
    apply_jsonld_metadata_mapping(distribution, ret)
    return ret
    
jsonld_metadata_mapping = {
    "title": "dcterms:title",
    "description": "dcterms:description",
    "keyword": "dcat:keyword",
    "modified": "dcterms:modified",
    "publisher": "dcterms:publisher",
    "contactPoint": "dcat:contactPoint",
    "mbox": "foaf:mbox",
    "identifier": "dcterms:identifier",
    "accessLevel": "pod:accessLevel",
        
    "bureauCode": "pod:bureauCode",
    "programCode": "pod:programCode",
    "accessLevelComment": "pod:accessLevelComment",
    "dataDictionary": "dcat:dataDictionary",

    "webService": "pod:webService",
    "format": "dcterms:format", # must be a dcterms:MediaTypeOrExtent
    "license": "dcterms:license",
    "spatial": "dcterms:spatial", # must be a dcterms:Location entity
    "temporal": "dcterms:temporal", # must be a dcterms:PeriodOfTime
        
    "issued": "dcterms:issued",
    "accrualPeriodicity": "dcterms:accrualPeriodicity", # must be a dcterms:Frequency 
    "language": "dcat:language", # must be an IRI
    "dataQuality": "pod:dataQuality",
    "theme": "dcat:theme",
    "references": "dcterms:references",
    "landingPage": "dcat:landingPage",
    "systemOfRecords": "pod:systemOfRecords",
}

jsonld_metadata_datatypes = {
    "modified": "http://www.w3.org/2001/XMLSchema#dateTime",
    "issued": "http://www.w3.org/2001/XMLSchema#dateTime",
}
    
def apply_jsonld_metadata_mapping(data, newdict):
    for k, v in data.items():
        # skip null/empty fields
        if v is None or (isinstance(v, str) and v.strip() == ""): continue
        
        # skip fields with no mapping to RDF
        if k not in jsonld_metadata_mapping: continue
        
        # specially handle literal fields with datatypes
        if k in jsonld_metadata_datatypes:
            # Convert ISO datetime format to xsd:dateTime format.
            if jsonld_metadata_datatypes[k] == "http://www.w3.org/2001/XMLSchema#dateTime":
                v = v.replace(" ", "T")
                
            v = OrderedDict([
               ("@value", v),
               ("@type", jsonld_metadata_datatypes[k]),
            ])
            
        # add value to collection
        newdict[jsonld_metadata_mapping[k]] = v

