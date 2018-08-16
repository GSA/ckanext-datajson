import re
import rfc3987 as rfc3987_url

# from the iso8601 package, plus ^ and $ on the edges
ISO8601_REGEX = re.compile(r"^([0-9]{4})(-([0-9]{1,2})(-([0-9]{1,2})"
                           r"((.)([0-9]{2}):([0-9]{2})(:([0-9]{2})(\.([0-9]+))?)?"
                           r"(Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?$")

TEMPORAL_REGEX_1 = re.compile(
    r'^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?'
    r'|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]'
    r'\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?(\/)([\+-]?\d{4}'
    r'(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|'
    r'(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]'
    r'\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
)

TEMPORAL_REGEX_2 = re.compile(
    r'^(R\d*\/)?([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\4([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])'
    r'(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)'
    r'([\.,]\d+(?!:))?)?(\18[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?(\/)'
    r'P(?:\d+(?:\.\d+)?Y)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?W)?(?:\d+(?:\.\d+)?D)?(?:T(?:\d+(?:\.\d+)?H)?'
    r'(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?S)?)?$'
)

TEMPORAL_REGEX_3 = re.compile(
    r'^(R\d*\/)?P(?:\d+(?:\.\d+)?Y)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?W)?(?:\d+(?:\.\d+)?D)?(?:T(?:\d+'
    r'(?:\.\d+)?H)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?S)?)?\/([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])'
    r'(\4([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))'
    r'([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\18[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])'
    r'([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
)

MODIFIED_REGEX_1 = re.compile(
    r'^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?'
    r'|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]'
    r'\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
)

MODIFIED_REGEX_2 = re.compile(
    r'^(R\d*\/)?P(?:\d+(?:\.\d+)?Y)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?W)?(?:\d+(?:\.\d+)?D)?(?:T(?:\d+(?:\.\d+)?H)?'
    r'(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?S)?)?$'
)

MODIFIED_REGEX_3 = re.compile(
    r'^(R\d*\/)?([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\4([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|'
    r'(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?'
    r'(\18[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?(\/)P(?:\d+(?:\.\d+)?Y)?(?:\d+(?:\.\d+)?M)?'
    r'(?:\d+(?:\.\d+)?W)?(?:\d+(?:\.\d+)?D)?(?:T(?:\d+(?:\.\d+)?H)?(?:\d+(?:\.\d+)?M)?(?:\d+(?:\.\d+)?S)?)?$'
)

ISSUED_REGEX = re.compile(
    r'^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?'
    r'|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]'
    r'\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$'
)

PROGRAM_CODE_REGEX = re.compile(r"^[0-9]{3}:[0-9]{3}$")

IANA_MIME_REGEX = re.compile(r"^[-\w]+/[-\w]+(\.[-\w]+)*([+][-\w]+)?$")

PRIMARY_IT_INVESTMENT_UII_REGEX = re.compile(r"^[0-9]{3}-[0-9]{9}$")

ACCRUAL_PERIODICITY_VALUES = (
    None, "R/P10Y", "R/P4Y", "R/P1Y", "R/P2M", "R/P3.5D", "R/P1D", "R/P2W", "R/P0.5W", "R/P6M",
    "R/P2Y", "R/P3Y", "R/P0.33W", "R/P0.33M", "R/PT1S", "R/PT1S", "R/P1M", "R/P3M",
    "R/P0.5M", "R/P4M", "R/P1W", "R/PT1H", "irregular")

LANGUAGE_REGEX = re.compile(
    r'^(((([A-Za-z]{2,3}(-([A-Za-z]{3}(-[A-Za-z]{3}){0,2}))?)|[A-Za-z]{4}|[A-Za-z]{5,8})(-([A-Za-z]{4}))?'
    r'(-([A-Za-z]{2}|[0-9]{3}))?(-([A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*(-([0-9A-WY-Za-wy-z](-[A-Za-z0-9]{2,8})+))*'
    r'(-(x(-[A-Za-z0-9]{1,8})+))?)|(x(-[A-Za-z0-9]{1,8})+)|'
    r'((en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo'
    r'|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)|'
    r'(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang)))$'
)

REDACTED_REGEX = re.compile(
    r'^(\[\[REDACTED).*?(\]\])$'
)

import lepl.apps.rfc3696

email_validator = lepl.apps.rfc3696.Email()

# load the OMB bureau codes on first load of this module
import urllib
import csv
import os

omb_burueau_codes = set()
# for row in csv.DictReader(urllib.urlopen("https://project-open-data.cio.gov/data/omb_bureau_codes.csv")):
#    omb_burueau_codes.add(row["Agency Code"] + ":" + row["Bureau Code"])

with open(os.path.join(os.path.dirname(__file__), "resources", "omb_bureau_codes.csv"), "r") as csvfile:
    for row in csv.DictReader(csvfile):
        omb_burueau_codes.add(row["Agency Code"] + ":" + row["Bureau Code"])


# main function for validation
def do_validation(doc, errors_array, seen_identifiers):
    errs = {}

    if type(doc) != list:
        add_error(errs, 0, "Bad JSON Structure",
                  "The file must be an array at its top level. "
                  "That means the file starts with an open bracket [ and ends with a close bracket ].")
    elif len(doc) == 0:
        add_error(errs, 0, "Catalog Is Empty", "There are no entries in your file.")
    else:
        for i, item in enumerate(doc):
            # Required

            dataset_name = "dataset %d" % (i + 1)

            # title
            if check_required_string_field(item, "title", 1, dataset_name, errs):
                dataset_name = '"%s"' % item.get("title", "").strip()

            # accessLevel # required
            if check_required_string_field(item, "accessLevel", 3, dataset_name, errs):
                if item["accessLevel"] not in ("public", "restricted public", "non-public"):
                    add_error(errs, 5, "Invalid Required Field Value",
                              "The field 'accessLevel' had an invalid value: \"%s\"" % item["accessLevel"],
                              dataset_name)

            # bureauCode # required
            if not is_redacted(item.get('bureauCode')):
                if check_required_field(item, "bureauCode", list, dataset_name, errs):
                    for bc in item["bureauCode"]:
                        if not isinstance(bc, (str, unicode)):
                            add_error(errs, 5, "Invalid Required Field Value", "Each bureauCode must be a string",
                                      dataset_name)
                        elif ":" not in bc:
                            add_error(errs, 5, "Invalid Required Field Value",
                                      "The bureau code \"%s\" is invalid. "
                                      "Start with the agency code, then a colon, then the bureau code." % bc,
                                      dataset_name)
                        elif bc not in omb_burueau_codes:
                            add_error(errs, 5, "Invalid Required Field Value",
                                      "The bureau code \"%s\" was not found in our list "
                                      "(https://project-open-data.cio.gov/data/omb_bureau_codes.csv)." % bc,
                                      dataset_name)

            # contactPoint # required
            if check_required_field(item, "contactPoint", dict, dataset_name, errs):
                cp = item["contactPoint"]
                # contactPoint - fn # required
                check_required_string_field(cp, "fn", 1, dataset_name, errs)

                # contactPoint - hasEmail # required
                if check_required_string_field(cp, "hasEmail", 9, dataset_name, errs):
                    if not is_redacted(cp.get('hasEmail')):
                        email = cp["hasEmail"].replace('mailto:', '')
                        if not email_validator(email):
                            add_error(errs, 5, "Invalid Required Field Value",
                                      "The email address \"%s\" is not a valid email address." % email,
                                      dataset_name)

            # description # required
            check_required_string_field(item, "description", 1, dataset_name, errs)

            # identifier #required
            if check_required_string_field(item, "identifier", 1, dataset_name, errs):
                if item["identifier"] in seen_identifiers:
                    add_error(errs, 5, "Invalid Required Field Value",
                              "The dataset identifier \"%s\" is used more than once." % item["identifier"],
                              dataset_name)
                seen_identifiers.add(item["identifier"])

            # keyword # required
            if isinstance(item.get("keyword"), (str, unicode)):
                if not is_redacted(item.get("keyword")):
                    add_error(errs, 5, "Update Your File!",
                              "The keyword field used to be a string but now it must be an array.", dataset_name)
            elif check_required_field(item, "keyword", list, dataset_name, errs):
                for kw in item["keyword"]:
                    if not isinstance(kw, (str, unicode)):
                        add_error(errs, 5, "Invalid Required Field Value",
                                  "Each keyword in the keyword array must be a string", dataset_name)
                    elif len(kw.strip()) == 0:
                        add_error(errs, 5, "Invalid Required Field Value",
                                  "A keyword in the keyword array was an empty string.", dataset_name)

            # modified # required
            if check_required_string_field(item, "modified", 1, dataset_name, errs):
                if not is_redacted(item['modified']) \
                        and not MODIFIED_REGEX_1.match(item['modified']) \
                        and not MODIFIED_REGEX_2.match(item['modified']) \
                        and not MODIFIED_REGEX_3.match(item['modified']):
                    add_error(errs, 5, "Invalid Required Field Value",
                              "The field \"modified\" is not in valid format: \"%s\"" % item['modified'], dataset_name)

            # programCode # required
            if not is_redacted(item.get('programCode')):
                if check_required_field(item, "programCode", list, dataset_name, errs):
                    for pc in item["programCode"]:
                        if not isinstance(pc, (str, unicode)):
                            add_error(errs, 5, "Invalid Required Field Value",
                                      "Each programCode in the programCode array must be a string", dataset_name)
                        elif not PROGRAM_CODE_REGEX.match(pc):
                            add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                      "One of programCodes is not in valid format (ex. 018:001): \"%s\"" % pc,
                                      dataset_name)

            # publisher # required
            if check_required_field(item, "publisher", dict, dataset_name, errs):
                # publisher - name # required
                check_required_string_field(item["publisher"], "name", 1, dataset_name, errs)

            # Required-If-Applicable

            # dataQuality # Required-If-Applicable
            if item.get("dataQuality") is None or is_redacted(item.get("dataQuality")):
                pass  # not required or REDACTED
            elif not isinstance(item["dataQuality"], bool):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'dataQuality' must be true or false, "
                          "as a JSON boolean literal (not the string \"true\" or \"false\").",
                          dataset_name)

            # distribution # Required-If-Applicable
            if item.get("distribution") is None:
                pass  # not required
            elif not isinstance(item["distribution"], list):
                if isinstance(item["distribution"], (str, unicode)) and is_redacted(item.get("distribution")):
                    pass
                else:
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                              "The field 'distribution' must be an array, if present.", dataset_name)
            else:
                for j, dt in enumerate(item["distribution"]):
                    if isinstance(dt, (str, unicode)):
                        if is_redacted(dt):
                            continue
                    distribution_name = dataset_name + (" distribution %d" % (j + 1))
                    # distribution - downloadURL # Required-If-Applicable
                    check_url_field(False, dt, "downloadURL", distribution_name, errs, allow_redacted=True)

                    # distribution - mediaType # Required-If-Applicable
                    if 'downloadURL' in dt:
                        if check_required_string_field(dt, "mediaType", 1, distribution_name, errs):
                            if not IANA_MIME_REGEX.match(dt["mediaType"]) \
                                    and not is_redacted(dt["mediaType"]):
                                add_error(errs, 5, "Invalid Field Value",
                                          "The distribution mediaType \"%s\" is invalid. "
                                          "It must be in IANA MIME format." % dt["mediaType"],
                                          distribution_name)

                    # distribution - accessURL # optional
                    check_url_field(False, dt, "accessURL", distribution_name, errs, allow_redacted=True)

                    # distribution - conformsTo # optional
                    check_url_field(False, dt, "conformsTo", distribution_name, errs, allow_redacted=True)

                    # distribution - describedBy # optional
                    check_url_field(False, dt, "describedBy", distribution_name, errs, allow_redacted=True)

                    # distribution - describedByType # optional
                    if dt.get("describedByType") is None or is_redacted(dt.get("describedByType")):
                        pass  # not required or REDACTED
                    elif not IANA_MIME_REGEX.match(dt["describedByType"]):
                        add_error(errs, 5, "Invalid Field Value",
                                  "The describedByType \"%s\" is invalid. "
                                  "It must be in IANA MIME format." % dt["describedByType"],
                                  distribution_name)

                    # distribution - description # optional
                    if dt.get("description") is not None:
                        check_required_string_field(dt, "description", 1, distribution_name, errs)

                    # distribution - format # optional
                    if dt.get("format") is not None:
                        check_required_string_field(dt, "format", 1, distribution_name, errs)

                    # distribution - title # optional
                    if dt.get("title") is not None:
                        check_required_string_field(dt, "title", 1, distribution_name, errs)

            # license # Required-If-Applicable
            check_url_field(False, item, "license", dataset_name, errs, allow_redacted=True)

            # rights # Required-If-Applicable
            # TODO move to warnings
            # if item.get("accessLevel") != "public":
            # check_string_field(item, "rights", 1, dataset_name, errs)

            # spatial # Required-If-Applicable
            # TODO: There are more requirements than it be a string.
            if item.get("spatial") is not None and not isinstance(item.get("spatial"), (str, unicode)):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'spatial' must be a string value if specified.", dataset_name)

            # temporal # Required-If-Applicable
            if item.get("temporal") is None or is_redacted(item.get("temporal")):
                pass  # not required or REDACTED
            elif not isinstance(item["temporal"], (str, unicode)):
                add_error(errs, 10, "Invalid Field Value (Optional Fields)",
                          "The field 'temporal' must be a string value if specified.", dataset_name)
            elif "/" not in item["temporal"]:
                add_error(errs, 10, "Invalid Field Value (Optional Fields)",
                          "The field 'temporal' must be two dates separated by a forward slash.", dataset_name)
            elif not TEMPORAL_REGEX_1.match(item['temporal']) \
                    and not TEMPORAL_REGEX_2.match(item['temporal']) \
                    and not TEMPORAL_REGEX_3.match(item['temporal']):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'temporal' has an invalid start or end date.", dataset_name)

            # Expanded Fields

            # accrualPeriodicity # optional
            if item.get("accrualPeriodicity") not in ACCRUAL_PERIODICITY_VALUES \
                    and not is_redacted(item.get("accrualPeriodicity")):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'accrualPeriodicity' had an invalid value.", dataset_name)

            # conformsTo # optional
            check_url_field(False, item, "conformsTo", dataset_name, errs, allow_redacted=True)

            # describedBy # optional
            check_url_field(False, item, "describedBy", dataset_name, errs, allow_redacted=True)

            # describedByType # optional
            if item.get("describedByType") is None or is_redacted(item.get("describedByType")):
                pass  # not required or REDACTED
            elif not IANA_MIME_REGEX.match(item["describedByType"]):
                add_error(errs, 5, "Invalid Field Value",
                          "The describedByType \"%s\" is invalid. "
                          "It must be in IANA MIME format." % item["describedByType"],
                          dataset_name)

            # isPartOf # optional
            if item.get("isPartOf"):
                check_required_string_field(item, "isPartOf", 1, dataset_name, errs)

            # issued # optional
            if item.get("issued") is not None and not is_redacted(item.get("issued")):
                if not ISSUED_REGEX.match(item['issued']):
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                              "The field 'issued' is not in a valid format.", dataset_name)

            # landingPage # optional
            check_url_field(False, item, "landingPage", dataset_name, errs, allow_redacted=True)

            # language # optional
            if item.get("language") is None or is_redacted(item.get("language")):
                pass  # not required or REDACTED
            elif not isinstance(item["language"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'language' must be an array, if present.", dataset_name)
            else:
                for s in item["language"]:
                    if not LANGUAGE_REGEX.match(s) and not is_redacted(s):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                  "The field 'language' had an invalid language: \"%s\"" % s, dataset_name)

            # PrimaryITInvestmentUII # optional
            if item.get("PrimaryITInvestmentUII") is None or is_redacted(item.get("PrimaryITInvestmentUII")):
                pass  # not required or REDACTED
            elif not PRIMARY_IT_INVESTMENT_UII_REGEX.match(item["PrimaryITInvestmentUII"]):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                          "The field 'PrimaryITInvestmentUII' must be a string "
                          "in 023-000000001 format, if present.", dataset_name)

            # references # optional
            if item.get("references") is None:
                pass  # not required or REDACTED
            elif not isinstance(item["references"], list):
                if isinstance(item["references"], (str, unicode)) and is_redacted(item.get("references")):
                    pass
                else:
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                              "The field 'references' must be an array, if present.", dataset_name)
            else:
                for s in item["references"]:
                    if not rfc3987_url.match(s) and not is_redacted(s):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                  "The field 'references' had an invalid rfc3987 URL: \"%s\"" % s, dataset_name)

                if len(item["references"]) != len(set(item["references"])):
                    add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                              "The field 'references' has duplicates", dataset_name)

            # systemOfRecords # optional
            check_url_field(False, item, "systemOfRecords", dataset_name, errs, allow_redacted=True)

            # theme #optional
            if item.get("theme") is None or is_redacted(item.get("theme")):
                pass  # not required or REDACTED
            elif not isinstance(item["theme"], list):
                add_error(errs, 50, "Invalid Field Value (Optional Fields)", "The field 'theme' must be an array.",
                          dataset_name)
            else:
                for s in item["theme"]:
                    if not isinstance(s, (str, unicode)):
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                  "Each value in the theme array must be a string", dataset_name)
                    elif len(s.strip()) == 0:
                        add_error(errs, 50, "Invalid Field Value (Optional Fields)",
                                  "A value in the theme array was an empty string.", dataset_name)

    # Form the output data.
    for err_type in sorted(errs):
        errors_array.append((
            err_type[1],  # heading
            [err_item + (" (%d locations)" % len(errs[err_type][err_item]) if len(errs[err_type][err_item]) else "")
             for err_item in sorted(errs[err_type], key=lambda x: (-len(errs[err_type][x]), x))
             ]))


def add_error(errs, severity, heading, description, context=None):
    s = errs.setdefault((severity, heading), {}).setdefault(description, set())
    if context: s.add(context)


def nice_type_name(data_type):
    if data_type == (str, unicode) or data_type in (str, unicode):
        return "string"
    elif data_type == list:
        return "array"
    else:
        return str(data_type)


def check_required_field(obj, field_name, data_type, dataset_name, errs):
    # checks that a field exists and has the right type
    if field_name not in obj:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is missing." % field_name, dataset_name)
        return False
    elif obj[field_name] is None:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is empty." % field_name, dataset_name)
        return False
    elif not isinstance(obj[field_name], data_type):
        add_error(errs, 5, "Invalid Required Field Value",
                  "The '%s' field must be a %s but it has a different datatype (%s)." % (
                      field_name, nice_type_name(data_type), nice_type_name(type(obj[field_name]))), dataset_name)
        return False
    elif isinstance(obj[field_name], list) and len(obj[field_name]) == 0:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is an empty array." % field_name, dataset_name)
        return False
    return True


def check_required_string_field(obj, field_name, min_length, dataset_name, errs):
    # checks that a required field exists, is typed as a string, and has a minimum length
    if not check_required_field(obj, field_name, (str, unicode), dataset_name, errs):
        return False
    elif len(obj[field_name].strip()) == 0:
        add_error(errs, 10, "Missing Required Fields", "The '%s' field is present but empty." % field_name,
                  dataset_name)
        return False
    elif len(obj[field_name].strip()) < min_length:
        add_error(errs, 100, "Invalid Field Value",
                  "The '%s' field is very short (min. %d): \"%s\"" % (field_name, min_length, obj[field_name]),
                  dataset_name)
        return False
    return True


def is_redacted(field):
    if isinstance(field, (str, unicode)) and REDACTED_REGEX.match(field):
        return True
    return False


def check_url_field(required, obj, field_name, dataset_name, errs, allow_redacted=False):
    # checks that a required or optional field, if specified, looks like a URL
    if not required and (field_name not in obj or obj[field_name] is None): return True  # not required, so OK
    if not check_required_field(obj, field_name, (str, unicode), dataset_name,
                                errs): return False  # just checking data type
    if allow_redacted and is_redacted(obj[field_name]): return True
    if not rfc3987_url.match(obj[field_name]):
        add_error(errs, 5, "Invalid Required Field Value",
                  "The '%s' field has an invalid rfc3987 URL: \"%s\"." % (field_name, obj[field_name]), dataset_name)
        return False
    return True
