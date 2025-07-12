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
    'reservering': ('Reservering', 'HAS_RESERVERING', 'id'),
    'zaal':        ('Zaal',        'HAS_ZAAL',        'id'),
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
    'aanvullende_gegevens': ('FractieAanvullendGegeven', 'HAS_AANVULLEND', 'id'),
}

REL_MAP_FRACTIE_ZETEL = {
    # Each zetel has at most one active FractieZetelPersoon record
    'fractie_zetel_persoon': ('FractieZetelPersoon', 'HAS_INCUMBENT', 'id'),
    'fractie_zetel_vacature': ('FractieZetelVacature', 'HAS_VACANCY', 'id'),
}

# From FractieZetelPersoon to the underlying Person
REL_MAP_FRACTIE_ZETEL_PERSOON = {
    'persoon': ('Persoon', 'IS_PERSON', 'id'),
}

REL_MAP_PERSOON = {
    'fractieleden':      ('FractieZetelPersoon',        'HAS_SEAT_ASSIGNMENT', 'id'),
    'contact_informaties': ('PersoonContactinformatie',  'HAS_CONTACTINFO',     'id'),
    'geschenken':        ('PersoonGeschenk',            'RECEIVED_GIFT',       'id'),
    'loopbaan':          ('PersoonLoopbaan',            'HAS_CAREER',          'id'),
    'nevenfuncties':     ('PersoonNevenfunctie',        'HAS_SIDEPOSITION',    'id'),
    'onderwijs':         ('PersoonOnderwijs',           'HAS_EDUCATION',       'id'),
    'reizen':            ('PersoonReis',                'HAS_TRAVEL',          'id'),
}

# Nested mapping for PersoonNevenfunctie → PersoonNevenfunctieInkomsten
REL_MAP_PERSOON_NEVENFUNCTIE = {
    'inkomsten': ('PersoonNevenfunctieInkomsten', 'HAS_INCOME', 'id'),
}

REL_MAP_COMMISSIE = {
    'zetels': ('CommissieZetel', 'HAS_ZETEL', 'id'),
    'contact_informaties': ('CommissieContactinformatie', 'HAS_CONTACTINFO', 'id'),
}

REL_MAP_COMMISSIE_ZETEL = {
    'personen_vast': ('CommissieZetelVastPersoon', 'HAS_MEMBER', 'id'),
    'personen_vervangend': ('CommissieZetelVervangerPersoon', 'HAS_TEMP_MEMBER', 'id'),
    'vacatures_vast': ('CommissieZetelVastVacature', 'HAS_VACANCY', 'id'),
    'vacatures_vervanger': ('CommissieZetelVervangerVacature', 'HAS_TEMP_VACANCY', 'id'),
}

REL_MAP_COMMISSIE_ZETEL_PERSOON = {
    'persoon': ('Persoon', 'IS_PERSON', 'id'),
}

REL_MAP_ZAAK_ACTOR = {
    'persoon':   ('Persoon',   'ACTED_AS_PERSOON',   'id'),
    'fractie':   ('Fractie',   'ACTED_AS_FRACTIE',   'id'),
    'commissie': ('Commissie', 'ACTED_AS_COMMISSIE', 'id'),
}

# --- TKApi Timeout Configuration ---
import os

# Timeout settings for TKApi requests (in seconds)
TKAPI_CONNECT_TIMEOUT = float(os.getenv('TKAPI_CONNECT_TIMEOUT', '15.0'))
TKAPI_READ_TIMEOUT = float(os.getenv('TKAPI_READ_TIMEOUT', '300.0'))
TKAPI_MAX_RETRIES = int(os.getenv('TKAPI_MAX_RETRIES', '3'))
TKAPI_BACKOFF_FACTOR = float(os.getenv('TKAPI_BACKOFF_FACTOR', '0.5'))

# --- New Entity Relationship Maps ---

REL_MAP_PERSOON_FUNCTIE = {
    'persoon': ('Persoon', 'PERSON_HAS_FUNCTION', 'id'),
    'fractie': ('Fractie', 'FUNCTION_FOR_FRACTIE', 'id'),
}

REL_MAP_KAMERSTUKDOSSIER = {
    'documenten': ('Document', 'CONTAINS_DOCUMENT', 'id'),
    'zaken': ('Zaak', 'CONTAINS_ZAAK', 'nummer'),
}

REL_MAP_ZAAL = {
    'activiteiten': ('Activiteit', 'HOSTS_ACTIVITEIT', 'id'),
    'vergaderingen': ('Vergadering', 'HOSTS_VERGADERING', 'id'),
    'reserveringen': ('Reservering', 'HAS_RESERVERING', 'id'),
}

REL_MAP_RESERVERING = {
    'zaal': ('Zaal', 'RESERVES_ZAAL', 'id'),
    'activiteit': ('Activiteit', 'FOR_ACTIVITEIT', 'id'),
    'vergadering': ('Vergadering', 'FOR_VERGADERING', 'id'),
}
