"""
Name Matcher for VLOS Processing

Handles sophisticated name matching between VLOS speakers and TK API Persoon entities
with enhanced surname handling including tussenvoegsel support.
"""

import re
from typing import Optional
from thefuzz import fuzz
from tkapi.persoon import Persoon

from ..config import VlosConfig
from ..models import XmlSpeaker, MatchResult, MatchType, SpeakerMatch


class NameMatcher:
    """Handles name matching for speakers"""
    
    @staticmethod
    def calculate_name_similarity(v_first: str, v_last: str, persoon: Persoon, config: VlosConfig) -> int:
        """Calculate similarity score between VLOS name and Persoon with enhanced surname handling"""
        
        score = 0
        
        if not (v_last and persoon.achternaam):
            return score
        
        v_last_lower = v_last.lower()
        
        # Build full surname including tussenvoegsel
        bare_surname = persoon.achternaam.lower()
        full_surname = NameMatcher._build_full_surname(persoon)
        
        # Pick best of bare vs full surname similarity
        ratio_bare = fuzz.ratio(v_last_lower, bare_surname)
        ratio_full = fuzz.ratio(v_last_lower, full_surname)
        best_ratio = max(ratio_bare, ratio_full)
        
        # Exact match on either variant → big boost
        if v_last_lower in [bare_surname, full_surname]:
            score += 60
        else:
            # Convert fuzzy similarity to 0-60 scale (dampen by 20)
            score += max(best_ratio - 20, 0)
        
        # Firstname / roepnaam boost
        v_first_lower = (v_first or "").lower()
        if v_first_lower:
            first_candidates = [
                c for c in [getattr(persoon, "roepnaam", None), getattr(persoon, "voornamen", None)] 
                if c
            ]
            if first_candidates:
                best_first = max((fuzz.ratio(v_first_lower, fc.lower()) for fc in first_candidates), default=0)
                if best_first >= config.matching.fuzzy_firstname_threshold:
                    score += 40
                elif best_first >= 60:
                    score += 20
        
        return min(score, 100)  # cap at 100
    
    @staticmethod
    def match_speaker(xml_speaker: XmlSpeaker, api_personen: list, config: VlosConfig) -> SpeakerMatch:
        """Match an XML speaker to API Persoon entities"""
        
        best_persoon = None
        best_score = 0
        reasons = []
        
        for persoon in api_personen:
            score = NameMatcher.calculate_name_similarity(
                xml_speaker.voornaam, 
                xml_speaker.achternaam, 
                persoon, 
                config
            )
            
            if score > best_score:
                best_score = score
                best_persoon = persoon
        
        # Determine match result
        if best_score >= config.matching.min_speaker_similarity_score:
            if best_score >= 90:
                match_type = MatchType.EXACT
                reasons.append("High confidence name match")
            else:
                match_type = MatchType.FUZZY
                reasons.append(f"Fuzzy name match (score: {best_score})")
            
            match_result = MatchResult(
                success=True,
                match_type=match_type,
                score=best_score,
                matched_entity=best_persoon,
                reasons=reasons,
                metadata={'similarity_score': best_score}
            )
            
            return SpeakerMatch(
                xml_speaker=xml_speaker,
                match_result=match_result,
                persoon_id=best_persoon.id,
                persoon_name=f"{best_persoon.roepnaam or best_persoon.voornamen} {best_persoon.achternaam}"
            )
        else:
            match_result = MatchResult(
                success=False,
                match_type=MatchType.NO_MATCH,
                score=best_score,
                matched_entity=None,
                reasons=[f"No suitable match found (best score: {best_score})"]
            )
            
            return SpeakerMatch(
                xml_speaker=xml_speaker,
                match_result=match_result
            )
    
    @staticmethod
    def _build_full_surname(persoon: Persoon) -> str:
        """Return full surname including tussenvoegsel (if any)"""
        full = f"{persoon.tussenvoegsel} {persoon.achternaam}".strip()
        return re.sub(r"\s+", " ", full).lower() 