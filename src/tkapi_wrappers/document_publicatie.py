"""TKApi wrapper classes for DocumentPublicatie and DocumentPublicatieMetadata.
These entity types are not provided by the upstream `tkapi` library but are
required to fully capture the publication chain of a Document.
"""
from types import MethodType
from tkapi.core import TKItem
from tkapi.filter import Filter

__all__ = [
    "DocumentPublicatie",
    "DocumentPublicatieMetadata",
]


class _BasePublication(TKItem):
    """Shared logic for publication-level entities."""

    @staticmethod
    def create_filter():
        # No special helper filters are needed
        return Filter()

    # Common fields ---------------------------------------------------------
    @property
    def identifier(self):
        return self.get_property_or_empty_string("Identifier")

    @property
    def document_type(self):
        return self.get_property_or_empty_string("DocumentType")

    @property
    def file_name(self):
        return self.get_property_or_empty_string("FileName")

    @property
    def url(self):
        # Url is already a property on TKItem, but here it contains the file
        # location of the publication – keep a dedicated alias for clarity.
        return self.get_property_or_empty_string("Url")

    @property
    def content_length(self):
        return self.get_property_or_none("ContentLength")

    @property
    def content_type(self):
        return self.get_property_or_empty_string("ContentType")

    @property
    def publicatie_datum(self):
        return self.get_date_from_datetime_or_none("PublicatieDatum")

    # Relationship back to versie ------------------------------------------
    @property
    def documentversie(self):
        from tkapi.document import DocumentVersie  # late import to avoid cycles
        return self.related_item(DocumentVersie)


class DocumentPublicatie(_BasePublication):
    type = "DocumentPublicatie"


class DocumentPublicatieMetadata(_BasePublication):
    type = "DocumentPublicatieMetadata"


# ---------------------------------------------------------------------------
# Monkey-patch convenience properties onto the original DocumentVersie class
# so callers can simply use `versie.publicaties`.
# ---------------------------------------------------------------------------
from tkapi.document import DocumentVersie as _TkDocumentVersie


def _get_publicaties(self):
    return self.related_items(DocumentPublicatie)


def _get_publicatie_metadata(self):
    return self.related_items(DocumentPublicatieMetadata, item_key="DocumentPublicatieMetadata")

# Attach the properties only once
if not hasattr(_TkDocumentVersie, "publicaties"):
    _TkDocumentVersie.publicaties = property(_get_publicaties)
if not hasattr(_TkDocumentVersie, "publicatie_metadata"):
    _TkDocumentVersie.publicatie_metadata = property(_get_publicatie_metadata) 