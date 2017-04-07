from .shared import *

def derive_form_fields(config, form_config, form, event, subj):
    """
    Here we derive the branching logic fields on the form
    level.
    This function is pretty beastly and should probably be refactored
    """
    for der_field in form_config['derived_fields'].values():
        target_field = der_field.get('field')
        uses = der_field.get('uses')
        der_type = der_field.get('type')
        status_type = 'status'
        hcv_status_type = 'hcv_status'
        date_completed_type = 'date_completed'

        if form_config != config['hcvrna']:
            if der_type == date_completed_type:
                if uses == 'redcap_event_name':
                    value = event
            elif type(uses) == type([]) and der_type == status_type:
                vals = [form.get(key) for key in uses if form.get(key)]
                if len(vals) == 2:
                    value = None
                elif target_field == 'inr_im_lbstat':
                    if len(vals) == 1:
                        value = None
                else:
                    value = "NOT_DONE"
        else:
            if der_type == hcv_status_type:
                try:
                    hcv_quant_field = form_config['csv_fields']['hcv_quant']
                    hcv_prescence_field = form_config['csv_fields']['hcv_prescence']
                    is_quantitative = float(form.get(hcv_quant_field))
                    value = 'Y'
                    del form[hcv_prescence_field]
                except Exception as err:
                    hcv_quant_field = form_config['csv_fields']['hcv_quant']
                    hcv_unit_field = form_config['csv_fields']['hcv_unit']
                    value = 'N'
                    del form[hcv_quant_field]
                    del form[hcv_unit_field]
            elif der_type == date_completed_type:
                if uses == 'redcap_event_name':
                    value = event


        if not value == None and target_field:
            form[target_field] = value

    return form

def derive_fields(config, data):
    """
    This delegates the derivation of each individual form to the derive_form_field
    """
    forms = config['forms']
    names = [form['form_name'] for form in forms]
    for flat_record in data:
        for form, form_name in zip(forms, names):
            form_data = flat_record.get(form_name)
            if form_data:
                subj = flat_record['dm_subjid']
                event = flat_record['redcap_event_name']
                flat_record[form_name] = derive_form_fields(config=config,
                                                            form_config=form,
                                                            form=form_data,
                                                            event=event,
                                                            subj=subj)
    return data

def derive_form_imported(config, data):
    """
    This derives the form imported field
    """
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]
    form_importeds = [form['form_imported'] for form in forms]
    for record in data:
        for name, imported in zip(form_names, form_importeds):
            if record.get(name):
                record[name][imported] = 'Y'
    return data


def pipeline(config, csv_data):
    """
    Given an optimus config and data in the csv output form,
    pass through all the processing steps in the pipeline in
    order.
    """
    forms = config['forms']

    pipeline = [
        build_flat_record,
        derive_fields,
        derive_form_imported,
        derive_form_completed,
        truncate_extra_events,
        flatten_forms
    ]

    kwargs = {
        'config': config,
        'data': csv_data
    }
    for func in pipeline:
        form_data = func(**kwargs)
        kwargs['data'] = form_data
    completed = kwargs['data']

    return completed
