def form_for_field(config, field):
    for form in config['forms']:
        if field in form['csv_fields'].values():
            return form['form_name']

def build_subj_event_form(config, data):
    subj_event_form = {}
    for datum in data:
        subj = datum['subj']
        event = datum['date']
        field = datum['field']
        form = form_for_field(config, field)

        if not subj_event_form.get(subj):
            subj_event_form[subj] = {event: {form: {}}}
        if not subj_event_form.get(subj).get(event):
            subj_event_form[subj][event] = {form: {}}
        if not subj_event_form.get(subj).get(event).get(form):
            subj_event_form[subj][event][form] = {}

        if field in config['csv_fields'].values():
            subj_event[subj][event][form][field] = datum['datum']
    return subj_event_form

def derive_fields(config, form):
    pass

def derive_completed_fields(config, data):
    # read the form template
    ## figure out what we need to update
    ## determine what we need to look at to get the info

    # get the info from the form data
    # fill out the completed fields correctly
    for subj in data.values():
        for event in subj.values():
            for form_key in event:
                form_config = [form for form in config['forms'] if form['form_name'] == form_key].pop()
                event[form_key] = derive_fields(form_config, event[form_key])
    pass

def derive_form_completed(config, data):
    # check the form
    # see what values we have and dont have
    # if we have something and something or something for all checks
    ## then okay
    pass

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
