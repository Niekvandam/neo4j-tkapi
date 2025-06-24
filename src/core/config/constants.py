REL_MAP_ZAAK = {
    'documenten':     ('Document',       'HAS_DOCUMENT',        'id'),
    'agendapunten':   ('Agendapunt',     'HAS_AGENDAPUNT',      'id'),
    'activiteiten':   ('Activiteit',     'HAS_ACTIVITEIT',      'id'),
    'besluiten':      ('Besluit',        'HAS_BESLUIT',         'id'),
    'actors':         ('ZaakActor',      'HAS_ACTOR',           'id'),
    'vervangen_door': ('Zaak',           'REPLACED_BY',         'nummer'),
}

REL_MAP_DOC = {
    'zaken':          ('Zaak',           'REFERS_TO_ZAAK',      'nummer'),
    'activiteiten':   ('Activiteit',     'HAS_ACTIVITEIT',      'id'),
    'actors':         ('DocumentActor',  'HAS_ACTOR',           'id'),
    'dossiers':       ('Dossier',        'HAS_DOSSIER',         'id'),
    'versies':        ('DocumentVersie', 'HAS_VERSIE',          'nummer'),
}

REL_MAP_ACTIVITEIT = {
    'documenten':     ('Document',        'HAS_DOCUMENT',        'id'),
    'zaken':          ('Zaak',            'PART_OF_ZAAK',        'nummer'),
    'agendapunten':   ('Agendapunt',      'HAS_AGENDAPUNT',      'id'),
    'actors':         ('ActiviteitActor', 'HAS_ACT_ACTOR',       'id'),
}

REL_MAP_ACTOR = {
    'activiteit': ('Activiteit', 'BELONGS_TO_ACTIVITEIT', 'id'),
    'persoon':    ('Persoon',    'ACTED_AS_PERSOON',      'id'),
    'fractie':    ('Fractie',    'ACTED_AS_FRACTIE',      'id'),
    'commissie':  ('Commissie',  'ACTED_AS_COMMISSIE',    'id'),
}
REL_MAP_TOEZEGGING = {
    'is_aanvulling_op':     ('Toezegging', 'SUPPLEMENTS',     'Id'),
    'is_aangevuld_vanuit':  ('Toezegging', 'SUPPLEMENTED_BY', 'Id'),
    'is_herhaling_van':     ('Toezegging', 'REITERATES',     'Id'),
    'is_herhaald_door':     ('Toezegging', 'REITERATED_BY',  'Id'),
    'is_wijziging_van':     ('Toezegging', 'MODIFIES',       'Id'),
    'is_gewijzigd_door':    ('Toezegging', 'MODIFIED_BY',    'Id'),
}

REL_MAP_BESLUIT = {
    'stemmingen': ('Stemming', 'HAS_STEMMING', 'id'),
    'zaken':      ('Zaak',     'ABOUT_ZAAK',    'nummer'),
    'agendapunt': ('Agendapunt','FROM_AGENDAPUNT','id')  
}

REL_MAP_ACTIVITEIT_SELF = {
    'vervangen_door':     ('Activiteit', 'REPLACED_BY', 'id'),
    'vervangen_vanuit':   ('Activiteit', 'REPLACED_FROM', 'id'),
    'voortgezet_in':      ('Activiteit', 'CONTINUED_IN', 'id'),
    'voortgezet_vanuit':  ('Activiteit', 'CONTINUED_FROM', 'id')
}
