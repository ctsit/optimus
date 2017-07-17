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
                    # blank out fields that could have been set previously but now may
                    # not be done. This shouldnt happen often since the past shouldnt
                    # change
                    for used_field in uses:
                        form[used_field] = ''

        else:
            if der_type == hcv_status_type:
                hcv_quant_field = form_config['csv_fields']['hcv_quant']
                hcv_unit_field = form_config['csv_fields']['hcv_unit']
                hcv_presence_field = form_config['csv_fields']['hcv_prescence']
                try:
                    is_quant = float(form.get(hcv_quant_field))
                    value = 'Y'
                    form[hcv_presence_field] = ''
                except:
                    value = 'N'
                    form[hcv_quant_field] = ''
                    form[hcv_unit_field] = ''

            elif der_type == date_completed_type:
                if uses == 'redcap_event_name':
                    value = event

        if target_field:
            form[target_field] = value if value else ''
            # Setting this to an emptry string is important as there could be
            # a change which gives a lab where there wasnt before and so we would
            # need to blank our the old derived value

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
        pull_events_left,
        truncate_extra_events,
        flatten_forms
    ]

    kwargs = {
        'config': config,
        'data': csv_data
    }
    for func in pipeline:
        print('Optimus pipeline doing: {}'.format(func))
        form_data = func(**kwargs)
        kwargs['data'] = form_data
    completed = kwargs['data']

    return completed
