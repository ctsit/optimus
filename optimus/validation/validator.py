import json
from cappy import API
from itertools import chain

class Validator(object):

    def __init__(self, metadata, form_config):
        """
        The metadata should be a deserialized redcap metadata.json response.
        The form_config is passed in the optimus.conf.yaml that specifies which
        fields belong to which forms
        """
        self.form_name = form_config['form_name']
        self.fields = list(self._get_fields_from_config(form_config))
        self.metadata = [(field['field_name'], field['form_name']) for field in metadata]

    def _get_fields_from_config(self, form_config):
        """
        We need to check both the fields that we get form the csv directly (csv_fields)
        as well as the derived fields which come from some operation applied to
        some of the csv fields.
        """
        csv_fields = [item for key, item in form_config['csv_fields'].items() if key != 'record_id']
        derived_fields = [item['field'] for key, item in form_config['derived_fields'].items()]
        return list(chain(csv_fields, derived_fields))

    def validate(self):
        """
        This function makes sure that the fields in the config are both
        in the metadata and that the form they are under in the config
        is the same as the form that they belong to in the redcap.
        """
        valid = True
        misconfigured_field_name = None
        names = [name for name, form in self.metadata]
        for my_field in self.fields:
            if my_field not in names:
                valid = False
                misconfigured_field_name = my_field
        if valid:
            forms = [form for name, form in self.metadata]
            for my_field in self.fields:
                field_form = forms[names.index(my_field)]
                valid = valid and self.form_name == field_form
                if not valid:
                    misconfigured_field_name = my_field
        return valid, misconfigured_field_name, self.form_name

def check_metadata_export_permission(response):
    """
    Depending on the redcap version and how it reacts this function
    may need to be made more robust. Sometimes redcap will happily
    respond with a 200 status_code but the actual content will be an
    error
    """
    if response.status_code == 403:
        exit("""

        !!! Optimus validation error !!!
        You have supplied the wrong URL, token pair in your config,
        or your token does not have the permission to export metadata.

        Make sure to check your auth information or download the metadata.json
        and use the metadata_path config to check a local copy of the metadata
        for validation.

        """)

def validate_config(config, metadata=None):
    """
    Instantiates a Validator and uses that with each form config along
    with the metadata to make sure that the field names in the form are
    the correct field names.
    """
    if not metadata:
        api = API(config['token'], config['redcap_url'], 'v6.16.0.json')
        response = api.export_metadata()
        check_metadata_export_permission(response)
        metadata = json.loads(str(response.content, 'utf-8'))
    for form in config['forms']:
        validator = Validator(metadata, form)
        is_valid, field_name, form_name = validator.validate()
        if not is_valid:
            print('Your configuration is not valid.')
            print("""
            The form {form_name} has {field_name} in it's configuration but
            {field_name} does not belong to {form_name} according to the metadata.
            """.format(form_name=form_name, field_name=field_name))
            exit()

def warning():
    print("""

        !!!WARNING!!!
        You are running optimus without a way to connect to RedCap.
        We CANNOT validate your config, and it could cause data loss.

        If you do not want this, add a redcap_url, and token parameter to the
        config yaml file.

        Alternatively, download the project's metadata in json format and place
        the path in the config under the 'metadata_path' key

        """)


