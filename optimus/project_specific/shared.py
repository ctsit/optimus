from copy import copy

def form_for_field(config, field):
    """
    Gets the form name which contains the field that is passed in
    """
    for form in config['forms']:
        if field in form['csv_fields'].values():
            return form['form_name']

def build_flat_record(config, data):
    """
    Takes a list of csv data outputs and puts it in a dictionary
    with fields:
    config['subject_field'],
    redcap_event_name,
    keys corresponding to the form name.

    these forms are dictionaries that contain the fields that correspond to
    each particular form.
    """
    flat_form_records = []
    subj_field = config['subject_field']
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
                subj_field: subj,
                'redcap_event_name': event,
                form: {}
            }
            flat_form_records.append(found)

        if not found.get(form):
            found[form] = {}
        found[form][field] = value

    return flat_form_records

def derive_form_completed(config, data):
    """
    From the config takes the form name and sets the form_complete
    value to 2. This is in shared because every form has this field
    automatically
    """
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]
    for record in data:
        for name in form_names:
            if record.get(name):
                record[name][name + '_complete'] = '2'
    return data

def __find_next(events, current_index, form_name):
    """
    Finds the next form in the event series or none. Removes
    that form from the event it was found in and returns the
    form

    This function is a helper for use in pull_events_left
    """
    form = None
    while current_index < len(events) - 1:
        current_index += 1
        if events[current_index].get(form_name):
            form = copy(events[current_index].get(form_name))
            del events[current_index][form_name]
            break
        else:
            pass
    return form


def pull_events_left(config, data):
    """
    There are some projects where the events across forms on a single
    subject are not temporally aligned. In this case, the first instance
    of a form should be on the first event
    """
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]
    subj_key = config['subject_field']

    subs = {}
    # for all subjects
    for item in data:
        if not subs.get(item[subj_key]):
            subs[item[subj_key]] = []
        subs[item[subj_key]].append(item)
    # get their events
    for subid, events in subs.items():
        events.sort(key=lambda e: e['redcap_event_name'])
        # look at the first event
        for index, event in enumerate(events):
            # for all forms
            no_more_forms = []
            for form in form_names:
                # if it is missing a form
                if not form in no_more_forms:
                    if not event.get(form):
                        # step forward to find that form
                        next_form = __find_next(events, index, form)
                        # if you can find the form, pull it back
                        if next_form:
                            event[form] = next_form
                        else:
                            no_more_forms.append(form)
    return data

def truncate_extra_events(config, data):
    """
    Some forms only have a set number of events. This cuts off the
    ones that come after the 'events' field in the form config
    """
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

def flatten_forms(config, data):
    """
    This takes the data which is in the form given by the build flat records
    and places each form's data in the top level record object.

    This is needed because of how redcap handles importing of flat records.
    If we didn't do this, then there is the possibility that something like
    INR would be on an event it wasnt supposed to be since it has less events
    it can go into
    """
    forms = config['forms']
    form_names = [form['form_name'] for form in forms]
    new_data = []

    for record in data:
        new_record = {}
        for key in record.keys():
            if key in form_names:
                form_name = key
                form = record[form_name]
                try:
                    for key in form.keys():
                        new_record[key] = form[key]
                except:
                    import pdb
                    pdb.set_trace()
            else:
                new_record[key] = record[key]
        new_data.append(new_record)
        for form_name in form_names:
            try:
                del new_data[-1][form_name]
            except:
                pass

    return new_data

