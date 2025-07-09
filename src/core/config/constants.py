REL_MAP_ZAAK = {
    'documenten':     ('Document',       'HAS_DOCUMENT',        'id'),
    'agendapunten':   ('Agendapunt',     'HAS_AGENDAPUNT',      'id'),
    'activiteiten':   ('Activiteit',     'HAS_ACTIVITEIT',      'id'),
    'besluiten':      ('Besluit',        'HAS_BESLUIT',         'id'),
    'actors':         ('ZaakActor',      'HAS_ACTOR',           'id'),
    'vervangen_door': ('Zaak',           'REPLACED_BY',         'nummer'),
    'dossier':        ('Dossier',        'HAS_DOSSIER',        'id'),
}

REL_MAP_DOC = {
    'zaken':          ('Zaak',           'REFERS_TO_ZAAK',      'nummer'),
    'activiteiten':   ('Activiteit',     'HAS_ACTIVITEIT',      'id'),
    'actors':         ('DocumentActor',  'HAS_ACTOR',           'id'),
    'dossiers':       ('Dossier',        'HAS_DOSSIER',         'id'),
    'versies':        ('DocumentVersie', 'HAS_VERSIE',          'id'),
}

REL_MAP_ACTIVITEIT = {
    'documenten':     ('Document',        'HAS_DOCUMENT',        'id'),
    'zaken':          ('Zaak',            'PART_OF_ZAAK',        'nummer'),
    'agendapunten':   ('Agendapunt',      'HAS_AGENDAPUNT',      'id'),
    'actors':         ('ActiviteitActor', 'HAS_ACT_ACTOR',       'id'),
    'voortouwcommissies': ('Commissie',        'HAS_VOORTOUWCOMMISSIE', 'id'),
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
    'agendapunt': ('Agendapunt','BELONGS_TO_AGENDAPUNT','id')
}

REL_MAP_ACTIVITEIT_SELF = {
    'vervangen_door':     ('Activiteit', 'REPLACED_BY', 'id'),
    'vervangen_vanuit':   ('Activiteit', 'REPLACED_FROM', 'id'),
    'voortgezet_in':      ('Activiteit', 'CONTINUED_IN', 'id'),
    'voortgezet_vanuit':  ('Activiteit', 'CONTINUED_FROM', 'id')
}

REL_MAP_DOCUMENT_ACTOR = {
    'persoon':   ('Persoon',   'ACTED_AS_PERSOON',   'id'),
    'fractie':   ('Fractie',   'ACTED_AS_FRACTIE',   'id'),
    'commissie': ('Commissie', 'ACTED_AS_COMMISSIE', 'id'),
}

REL_MAP_DOCUMENT_VERSIE = {
    'publicaties':        ('DocumentPublicatie',        'HAS_PUBLICATIE',          'id'),
    'publicatie_metadata': ('DocumentPublicatieMetadata', 'HAS_PUBLICATIE_METADATA', 'id'),
}

REL_MAP_FRACTIE = {
    'zetels': ('FractieZetel', 'HAS_ZETEL', 'id'),
}

REL_MAP_FRACTIE_ZETEL = {
    # Each zetel has at most one active FractieZetelPersoon record
    'fractie_zetel_persoon': ('FractieZetelPersoon', 'HAS_INCUMBENT', 'id'),
}

# From FractieZetelPersoon to the underlying Person
REL_MAP_FRACTIE_ZETEL_PERSOON = {
    'persoon': ('Persoon', 'IS_PERSON', 'id'),
}

REL_MAP_PERSOON = {
    'fractieleden': ('FractieZetelPersoon', 'HAS_SEAT_ASSIGNMENT', 'id'),
}

# --- TKApi Timeout Configuration ---
import os

# Timeout settings for TKApi requests (in seconds)
TKAPI_CONNECT_TIMEOUT = float(os.getenv('TKAPI_CONNECT_TIMEOUT', '15.0'))
TKAPI_READ_TIMEOUT = float(os.getenv('TKAPI_READ_TIMEOUT', '300.0'))
TKAPI_MAX_RETRIES = int(os.getenv('TKAPI_MAX_RETRIES', '3'))
TKAPI_BACKOFF_FACTOR = float(os.getenv('TKAPI_BACKOFF_FACTOR', '0.5'))
