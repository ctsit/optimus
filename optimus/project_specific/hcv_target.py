from .shared import *

def process_hcv_values(quant, unit, presence):
    """
    In the processing of hcv_rna labs there are a couple of things that
    could happen.

    - the presence or absence of hcv rna along with the quantitative measure
    will be in the same lab
    - or the two will be split into different labs

    This poses some unique difficulties because the redcap form requires some
    branching logic. If there is a quantifiable result, then the hcv_im_supplb_hcvquant
    field needs to be set to 'Y' otherwise it should be set to 'N'

    You can determine this by the value of the hcv_im_supplb_hcvdtct field.
    If it is DETECTED then we want to have yes. Anything else is N
    """
    has_hcv = 'DETECTED'
    no_hcv = 'BLOQ'
    cannot_determine = 'NOT SPECIFIED'
    states = [has_hcv, no_hcv, cannot_determine]

    if presence in states:
        # we processed or got presence from another lab or got the right
        # data from some other process so, we dont need to introspect the
        # quant or unit variables
        return quant, unit, presence

    if not presence in states:
        try:
            float(quant)
            return quant, unit, has_hcv
        except:
            if 'not detected' in str(quant).lower():
                return quant, unit, no_hcv
            else:
                return quant, unit, cannot_determine


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
                # this section is for the hcv_im_supplb_hcvquant field
                hcv_quant_field = form_config['csv_fields']['hcv_quant']
                hcv_unit_field = form_config['csv_fields']['hcv_unit']
                hcv_presence_field = form_config['csv_fields']['hcv_presence']

                quant, unit, presence = process_hcv_values(form[hcv_quant_field],
                                                           form[hcv_unit_field],
                                                           form[hcv_presence_field])

                value = 'Y' if presence == 'DETECTED' else 'N'

                if value == 'N':
                    form[hcv_quant_field] = ''
                    form[hcv_unit_field] = ''
                    form[hcv_presence_field] = presence
                else:
                    form[hcv_quant_field] = quant
                    form[hcv_unit_field] = unit
                    form[hcv_presence_field] = ''


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
