"""
TK API Extractor for VLOS Processing

Handles retrieval of entities from the TK API for matching against VLOS XML data.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Optional, Any
import re

from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingSoort
from tkapi.activiteit import Activiteit
from tkapi.persoon import Persoon
from tkapi.zaak import Zaak
from tkapi.dossier import Dossier
from tkapi.document import Document
from tkapi.agendapunt import Agendapunt
from tkapi.stemming import Stemming
from tkapi.besluit import Besluit

from ..config import VlosConfig
from ..models import XmlVergadering


class ApiExtractor:
    """Extracts entities from TK API for matching"""
    
    def __init__(self, config: VlosConfig, api: TKApi):
        self.config = config
        self.api = api
    
    def find_canonical_vergadering(self, xml_vergadering: XmlVergadering) -> Optional[Vergadering]:
        """Find the canonical Vergadering from TK API that matches the XML vergadering"""
        target_date = xml_vergadering.datum
        if not target_date:
            return None
        
        # Create date range with buffer
        buffer = self.config.time.vergadering_lookup_buffer
        utc_start = target_date - buffer - timedelta(hours=self.config.time.local_timezone_offset_hours)
        utc_end = target_date + buffer - timedelta(hours=self.config.time.local_timezone_offset_hours)
        
        # Set up API filter
        v_filter = Vergadering.create_filter()
        v_filter.filter_date_range(begin_datetime=utc_start, end_datetime=utc_end)
        
        # Add soort filter if available
        if xml_vergadering.soort:
            if xml_vergadering.soort.lower() == 'plenair':
                v_filter.filter_soort(VergaderingSoort.PLENAIR)
            elif xml_vergadering.soort.lower() == 'commissie':
                v_filter.filter_soort(VergaderingSoort.COMMISSIE)
        
        # Add nummer filter if available
        if xml_vergadering.nummer:
            try:
                v_filter.add_filter_str(f'VergaderingNummer eq {int(xml_vergadering.nummer)}')
            except ValueError:
                pass
        
        # Retrieve candidates
        Vergadering.expand_params = ['Verslag']
        vergaderingen = self.api.get_items(
            Vergadering, 
            filter=v_filter, 
            max_items=self.config.processing.max_candidate_vergaderingen
        )
        Vergadering.expand_params = None
        
        return vergaderingen[0] if vergaderingen else None
    
    def get_candidate_activities(self, canonical_vergadering: Vergadering) -> List[Activiteit]:
        """Get candidate activities for matching from TK API"""
        # Create time-based filter with buffer
        time_buffer = self.config.time.api_time_buffer
        start_utc = (canonical_vergadering.begin - time_buffer).astimezone(timezone.utc)
        end_utc = (canonical_vergadering.einde + time_buffer).astimezone(timezone.utc)
        
        act_filter = Activiteit.create_filter()
        act_filter.filter_date_range(begin_datetime=start_utc, end_datetime=end_utc)
        
        return self.api.get_items(
            Activiteit, 
            filter=act_filter, 
            max_items=self.config.processing.max_candidate_activities
        )

    def get_agendapunten_for_activity(self, activity_id: str) -> List[Agendapunt]:
        """Get Agendapunten associated with a specific activity"""
        try:
            af = Agendapunt.create_filter()
            # Use OData filter string for activity ID - proper GUID syntax
            af.add_filter_str(f"Activiteit/Id eq {activity_id}")
            # Expand to get related entities
            Agendapunt.expand_params = ['Zaak', 'Besluit', 'Document', 'Activiteit']
            agendapunten = self.api.get_items(Agendapunt, filter=af, max_items=50)
            Agendapunt.expand_params = None
            return agendapunten
        except Exception as e:
            print(f"❌ Error getting agendapunten for activity {activity_id}: {e}")
            return []

    def get_stemmingen_for_agendapunt(self, agendapunt_id: str) -> List[Stemming]:
        """Get Stemmingen (votes) for a specific agendapunt"""
        try:
            sf = Stemming.create_filter()
            # Use OData filter string for agendapunt ID - proper GUID syntax
            sf.add_filter_str(f"Agendapunt/Id eq {agendapunt_id}")
            # Expand to get fractie and zaak info
            Stemming.expand_params = ['Fractie', 'FractieZetel', 'Agendapunt']
            stemmingen = self.api.get_items(Stemming, filter=sf, max_items=100)
            Stemming.expand_params = None
            return stemmingen
        except Exception as e:
            print(f"❌ Error getting stemmingen for agendapunt {agendapunt_id}: {e}")
            return []

    def get_besluiten_for_agendapunt(self, agendapunt_id: str) -> List[Besluit]:
        """Get Besluiten (decisions) for a specific agendapunt"""
        try:
            bf = Besluit.create_filter()
            # Use OData filter string for agendapunt ID - proper GUID syntax
            bf.add_filter_str(f"Agendapunt/Id eq {agendapunt_id}")
            # Expand to get related info
            Besluit.expand_params = ['Agendapunt', 'Zaak']
            besluiten = self.api.get_items(Besluit, filter=bf, max_items=20)
            Besluit.expand_params = None
            return besluiten
        except Exception as e:
            print(f"❌ Error getting besluiten for agendapunt {agendapunt_id}: {e}")
            return []
    
    def find_persoon_by_name(self, first_name: str, last_name: str, actor_persons: List[Any] = None) -> Optional[Persoon]:
        """Find Persoon by name with priority for activity actors"""
        
        # Priority 1: Check activity actors first if provided
        if actor_persons:
            best_match = self._find_best_persoon_from_actors(first_name, last_name, actor_persons)
            if best_match:
                return best_match
        
        # Priority 2: General Persoon search
        if not last_name:
            return None
        
        # Try exact achternaam search first
        pf = Persoon.create_filter()
        pf.filter_achternaam(last_name)
        candidates = self.api.get_items(Persoon, filter=pf, max_items=20)
        
        if candidates:
            best_match = self._calculate_best_persoon_match(first_name, last_name, candidates)
            if best_match:
                return best_match
        
        # Fallback: search by contains main surname token
        main_last_token = last_name.strip().split()[-1]
        pf = Persoon.create_filter()
        safe_last = main_last_token.replace("'", "''")
        pf.add_filter_str(f"contains(tolower(Achternaam), '{safe_last.lower()}')")
        
        candidates = self.api.get_items(Persoon, filter=pf, max_items=self.config.processing.max_persoon_candidates)
        
        if candidates:
            return self._calculate_best_persoon_match(first_name, last_name, candidates)
        
        return None
    
    def find_zaak_with_fallback(self, dossiernummer: str, stuknummer: str) -> dict:
        """Find Zaak with multi-tier fallback logic"""
        result = {
            'zaak': None,
            'dossier': None, 
            'document': None,
            'match_type': 'no_match',
            'success': False
        }
        
        # Tier 1: Try to find specific Zaak
        zaak = self._find_best_zaak(dossiernummer, stuknummer)
        if zaak:
            result['zaak'] = zaak
            result['match_type'] = 'zaak'
            result['success'] = True
            return result
        
        # Tier 2: Dossier fallback
        if dossiernummer:
            dossier = self._find_best_dossier(dossiernummer)
            if dossier:
                result['dossier'] = dossier
                result['match_type'] = 'dossier_fallback'
                result['success'] = True
                
                # Also try to find document within this dossier
                if stuknummer:
                    num, toevoeg = self._split_dossier_code(dossiernummer)
                    document = self._find_best_document(num, toevoeg, stuknummer)
                    if document:
                        result['document'] = document
                
                return result
        
        return result
    
    def _find_best_zaak(self, dossiernummer: str, stuknummer: str) -> Optional[Zaak]:
        """Find best matching Zaak"""
        if not dossiernummer and not stuknummer:
            return None
        
        zf = Zaak.create_filter()
        
        dnr_int = self._safe_int(dossiernummer)
        if dnr_int is not None:
            zf.filter_kamerstukdossier(dnr_int)
        elif dossiernummer:
            zf.filter_nummer(dossiernummer)
        
        snr_int = self._safe_int(stuknummer)
        if snr_int is not None:
            zf.filter_document(snr_int)
        elif stuknummer:
            zf.filter_volgnummer(stuknummer)
        
        candidates = self.api.get_items(Zaak, filter=zf, max_items=self.config.processing.max_zaak_candidates)
        if not candidates:
            return None
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Prefer exact dossier+stuk number combo
        for z in candidates:
            if (dnr_int and self._safe_int(z.dossier.nummer) == dnr_int) and (
                snr_int is None or self._safe_int(z.volgnummer) == snr_int
            ):
                return z
        
        return candidates[0]
    
    def _find_best_dossier(self, dossier_code: str) -> Optional[Dossier]:
        """Find best matching Dossier"""
        num, toevoeg = self._split_dossier_code(dossier_code or "")
        if num is None:
            return None
        
        df = Dossier.create_filter()
        df.filter_nummer(num)
        if toevoeg:
            df.filter_toevoeging(toevoeg)
        
        items = self.api.get_items(Dossier, filter=df, max_items=5)
        return items[0] if items else None
    
    def _find_best_document(self, dossier_num: int, dossier_toevoeging: str, stuknummer: str) -> Optional[Document]:
        """Find best matching Document"""
        snr_int = self._safe_int(stuknummer)
        if snr_int is None:
            return None
        
        df = Document.create_filter()
        df.filter_volgnummer(snr_int)
        if dossier_num:
            df.filter_dossier(dossier_num, dossier_toevoeging)
        
        docs = self.api.get_items(Document, filter=df, max_items=5)
        return docs[0] if docs else None
    
    def _find_best_persoon_from_actors(self, first_name: str, last_name: str, actor_persons: List[Any]) -> Optional[Persoon]:
        """Find best matching Persoon from activity actors"""
        from ..matchers.name_matcher import NameMatcher
        
        best_persoon = None
        best_score = 0
        
        for actor in actor_persons or []:
            persoon = getattr(actor, "persoon", None)
            if not persoon:
                continue
            
            score = NameMatcher.calculate_name_similarity(first_name, last_name, persoon, self.config)
            if score > best_score:
                best_score = score
                best_persoon = persoon
        
        return best_persoon if best_score >= self.config.matching.min_speaker_similarity_score else None
    
    def _calculate_best_persoon_match(self, first_name: str, last_name: str, candidates: List[Persoon]) -> Optional[Persoon]:
        """Calculate best Persoon match from candidates"""
        from ..matchers.name_matcher import NameMatcher
        
        best_persoon = None
        best_score = 0
        
        for persoon in candidates:
            score = NameMatcher.calculate_name_similarity(first_name, last_name, persoon, self.config)
            if score > best_score:
                best_score = score
                best_persoon = persoon
        
        return best_persoon if best_score >= self.config.matching.min_speaker_similarity_score else None
    
    def _split_dossier_code(self, code: str) -> tuple:
        """Split dossier code into number and toevoeging"""
        dossier_regex = re.compile(r"^(\d+)(?:[-\s]?([A-Za-z0-9]+))?$")
        m = dossier_regex.match(code.strip()) if code else None
        if not m:
            return None, None
        nummer = self._safe_int(m.group(1))
        toevoeg = m.group(2) or None
        return nummer, toevoeg
    
    def _safe_int(self, val: str) -> Optional[int]:
        """Safely convert string to int"""
        try:
            return int(val)
        except (TypeError, ValueError):
            return None 