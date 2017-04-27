from .hcv_target import build_flat_record, derive_fields, truncate_extra_events, flatten_forms

def pipeline(config, csv_data):
    forms = config['forms']

    pipeline = [
        build_flat_record,
        derive_fields,
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
