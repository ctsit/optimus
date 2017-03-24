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

def derive_lbstat_fields(config, form_config, form, event, subj):
    for der_field in form_config['derived_fields'].values():
        target_field = der_field.get('field')
        uses = der_field.get('uses')
        der_type = der_field.get('type')
        status_type = 'status'

        if form != config['hcvrna']:
            if type(uses) != type([]):
                if uses == 'redcap_event_name':
                    value = event
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
                flat_record[form_name] = derive_lbstat_fields(config=config,
                                                              form_config=form,
                                                              form=form_data,
                                                              event=event,
                                                              subj=subj)
    return data

def derive_form_completed(config, data):
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]
    for record in data:
        for name in form_names:
            if record.get(name):
                record[name][name + '_completed'] = 'Y'
    return data

def truncate_extra_events(config, data):
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]

    person_forms = {}
    # lists of person forms,
    for record in data:
        person = record['dm_subjid']
        if not person_forms.get(person):
            person_forms[person] = {form : [] for form in form_names}
        for name in form_names:
            rec = record.get(name)
            event = record.get('redcap_event_name')
            if rec:
                person_forms[person][name].append((rec, event))
    # when a single list is too long,
    for name in form_names:
        form_config = [form for form in forms if form['form_name'] == name].pop()
        for person in person_forms.values():
            max_events = form_config['events']
            # sort by the event and truncate the list
            recs = person.get(name)
            if recs:
                person[name].sort(key=lambda d : d[1])
                person[name] = [rec[0] for rec in person[name][0:(max_events - 1)]]

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
    completed['form_name'] = kwargs['data']

    return completed
