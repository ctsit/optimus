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
        return json.dumps(flatlist)

    def transform_and_roll_out(self):
        if (self.outformat == 'json'):
            return self.jsonify()

