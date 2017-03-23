def form_for_field(config, field):
    for form in config['forms']:
        if field in form['csv_fields'].values():
            return form['form_name']

def build_flat_record(config, data):
    flat_form_records = []
    for datum in data:
        subj = datum['subj']
        event = datum['date']
        field = datum['field']
        form = form_for_field(config, field)
        value = datum['datum']

        found = False
        for record in flat_form_records:
            subj_same = record['dm_subjid'] == subj
            event_same = record['redcap_event_name'] == event

            if subj_same and event_same:
                found = record
        if not found:
            found = {
                'dm_subjid': subj,
                'redcap_event_name': event,
                form: {}
            }
            flat_form_records.append(found)

        if not found.get(form):
            found[form] = {}
        found[form][field] = value

    return flat_form_records

def derive_lbstat_fields(config, form, event, subj):
    for der_field in config['derived_fields']:
        target_field = der_field.get('field')
        uses = der_field.get('uses')
        der_type = der_field.get('type')
        status_type = config['derived_types']['status']

        if form != config['hcvrna']:
            if type(uses) != type([]):
                value = form[config['csv_fields'][uses]]
            elif type(uses) == type([]) and der_type == status_type:
                vals = [form.get(key) for key in uses if form.get(key)]
                if len(vals) == 2:
                    value = None
                else:
                    value = "NOT_DONE"
        else:
            # do something special wrt the multiple dependant fields
            pass

        if not value == None and target_field:
            form[target_field] = value

    return form

def derive_completed_fields(config, data):
    forms = config['forms']
    names = [form['form_name'] for form in forms]
    for flat_record in data:
        for form, form_name in zip(forms, names):
            form_data = flat_record.get(form_name)
            if form_data:
                subj = flat_record['dm_subjid']
                event = flat_record['redcap_event_name']
                flat_record[form_name] = derive_lbstat_fields(form,
                                                              form_data,
                                                              event=event,
                                                              subj=)
    return data

def derive_form_completed(config, data):
    # read through through the flat records
    # go through each form
    # add the field that corresponds to complete
    for record in data

 def truncate_extra_events(config, data):
    # check to see how many events we have
    # if we have more than that
    ## get rid of them
    pass

def pipeline(config, csv_data):
    """
    Given an optimus config and data in the csv output form,
    pass through all the processing steps in the pipeline in
    order.
    """
    forms = config['forms']

    pipeline = [
        build_subject_event_form
        derive_completed_fields,
        derive_form_completed,
        truncate_extra_events
    ]

    completed = {}
    kwargs = {
        'config': config,
        'data': csv_data
    }
    for func in pipeline:
        form_data = func(**kwargs)
        kwargs['data'] = form_data
    completed[form['form_name']] = kwargs['data']

    return completed
