import json

class StagingArea(object):
    def __init__(self, subjects={}, outformat='json', subject_field='dm_subjid', event_field='redcap_event_name'):
        self.subjects = subjects
        self.outformat = outformat
        self.subject_field = subject_field
        self.event_field = event_field

    def get_subject(self, subject_id):
        subject = self.subjects.get(subject_id)
        if not subject:
            self.subjects[subject_id] = {}
            return self.subjects[subject_id]
        else:
            return subject

    def get_event(self, subject, event_key):
        event = subject.get(event_key)
        if not event:
            subject[event_key] = {}
            return subject[event_key]
        else:
            return event

    def add_data(self, subject_key, event_key, **data):
        subject = self.get_subject(subject_key)
        event = self.get_event(subject, event_key)
        for key in data.keys():
            event[key] = data[key]

    def jsonify(self):
        flatlist = []
        for subject_key in self.subjects.keys():
            subject = self.subjects[subject_key]
            for event_key in subject.keys():
                event = subject[event_key]
                event[self.subject_field] = subject_key
                event[self.event_field] = event_key
                flatlist.append(event)
        flatlist.sort(key=lambda record: record['dm_subjid']+record['redcap_event_name'])
        return json.dumps(flatlist, sort_keys=True)

    def event_condense(self):
        all_fields = set()
        for subject_key in self.subjects.keys():
            subject = self.subjects[subject_key]
            for event_key in subject.keys():
                event = subject[event_key]
                all_fields = all_fields.union(event.keys())
        for subject_key in self.subjects.keys():
            subject = self.subjects[subject_key]
            event_keys = list(subject.keys())
            event_keys.sort()
            for index, key in enumerate(event_keys):
                event = subject[key]
                fields = set(event.keys())
                missing = all_fields.difference(fields)
                if len(missing):
                    for field in missing:
                        # look to the next event
                        remaining_events = [subject[event_key] for event_key in event_keys if event_keys.index(event_key) > index]
                        # if it has the field, take it and move to next field
                        for item in remaining_events:
                            if item.get(field):
                                event[field] = item[field]
                                del item[field]
                                break


    def transform_and_roll_out(self):
        self.event_condense()
        if (self.outformat == 'json'):
            return self.jsonify()

