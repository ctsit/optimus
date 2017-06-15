import json
from cappy import API
from itertools import chain

class Validator(object):

    def __init__(self, metadata, form_config):
        self.form_name = form_config['form_name']
        self.fields = self._get_fields_from_config(form_config)
        self.metadata = metadata

    def _get_fields_from_config(form_config):
        csv_fields = [item for key, item in form_config['csv_fields'].items()]
        derived_fields = [item['field'] for key, item in form_config['derived_fields'].items()]
        return list(chain(csv_fields, derived_fields))

    def validate(self):
        valid = True
        field_name = None
        for my_field in self.fields:
            for redcap_field in self.metadata:
                if my_field == recap_field['field_name']:
                    valid = valid and self.form_name == redcap_field['form_name']:
                    if not valid:
                        field_name = my_field
                        break
        return valid, field_name, self.form_name

def validate_config(config, metadata_path=None):
    if not metadata_path:
        api = API(config['token'], config['redcap_url'], 'v6.16.0.json')
        metadata = json.loads(str(api.export_metadata().content, 'utf-8'))
    else:
        with open(metadata_path, 'r') as infile:
            metadata = json.loads(infile.read())
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


