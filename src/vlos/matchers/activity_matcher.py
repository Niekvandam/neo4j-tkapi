"""
Activity Matcher for VLOS Processing

Handles sophisticated activity matching between VLOS XML activities and TK API Activiteit entities
using time proximity, type matching, and topic similarity scoring.
"""

import re
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Any
from thefuzz import fuzz
from tkapi.activiteit import Activiteit

from ..config import VlosConfig
from ..models import XmlActivity, MatchResult, MatchType, ActivityMatch
from ..transformers.topic_normalizer import TopicNormalizer


class ActivityMatcher:
    """Handles activity matching with sophisticated scoring"""
    
    def __init__(self, config: VlosConfig):
        self.config = config
        self.topic_normalizer = TopicNormalizer(config)
    
    def match_activity(self, xml_activity: XmlActivity, api_activities: List[Activiteit], 
                      canonical_vergadering) -> ActivityMatch:
        """Match XML activity to API activities using sophisticated scoring"""
        
        best_match = None
        best_score = 0.0
        potential_matches = []
        
        for api_act in api_activities:
            score, reasons = self._calculate_activity_match_score(
                xml_activity, api_act, canonical_vergadering
            )
            
            potential_matches.append({
                'score': score,
                'reasons': reasons,
                'api_activity': api_act,
                'api_activity_id': api_act.id
            })
            
            if score > best_score:
                best_score = score
                best_match = api_act
        
        # Sort potential matches by score
        potential_matches.sort(key=lambda x: x['score'], reverse=True)
        
        # Determine if we accept the match
        accept_match = self._should_accept_match(best_score, potential_matches)
        
        if accept_match and best_match:
            if best_score >= self.config.matching.min_match_score_for_activiteit + 2:
                match_type = MatchType.EXACT
            else:
                match_type = MatchType.FUZZY
            
            match_result = MatchResult(
                success=True,
                match_type=match_type,
                score=best_score,
                matched_entity=best_match,
                reasons=potential_matches[0]['reasons'],
                metadata={'all_scores': potential_matches}
            )
            
            return ActivityMatch(
                xml_activity=xml_activity,
                match_result=match_result,
                api_activity_id=best_match.id,
                potential_matches=potential_matches
            )
        else:
            match_result = MatchResult(
                success=False,
                match_type=MatchType.NO_MATCH,
                score=best_score,
                matched_entity=None,
                reasons=[f"No strong match found (best score: {best_score:.2f})"],
                metadata={'all_scores': potential_matches}
            )
            
            return ActivityMatch(
                xml_activity=xml_activity,
                match_result=match_result,
                potential_matches=potential_matches
            )
    
    def _calculate_activity_match_score(self, xml_activity: XmlActivity, api_activity: Activiteit, 
                                      canonical_vergadering) -> Tuple[float, List[str]]:
        """Calculate comprehensive match score between XML and API activities"""
        
        score = 0.0
        reasons = []
        
        # 1. Time proximity scoring
        time_score, time_reason = self._evaluate_time_match(
            xml_activity, api_activity, canonical_vergadering
        )
        score += time_score
        if time_score > 0:
            reasons.append(time_reason)
        
        # 2. Soort (type) comparison
        soort_score, soort_reason = self._evaluate_soort_match(
            xml_activity.soort, api_activity
        )
        score += soort_score
        if soort_score > 0:
            reasons.append(soort_reason)
        
        # 3. Onderwerp/titel similarity
        topic_score, topic_reasons = self._evaluate_topic_match(
            xml_activity, api_activity
        )
        score += topic_score
        reasons.extend(topic_reasons)
        
        return score, reasons
    
    def _evaluate_time_match(self, xml_activity: XmlActivity, api_activity: Activiteit, 
                           canonical_vergadering) -> Tuple[float, str]:
        """Evaluate time-based matching between activities"""
        
        # Use XML times if available, otherwise fall back to vergadering times
        xml_start = xml_activity.start_time or canonical_vergadering.begin
        xml_end = xml_activity.end_time or canonical_vergadering.einde
        
        if not (xml_start and api_activity.begin and api_activity.einde):
            return 0.0, 'Missing time data'
        
        # Convert to UTC for comparison
        xml_start_utc = self._get_utc_datetime(xml_start)
        xml_end_utc = self._get_utc_datetime(xml_end or (xml_start + timedelta(minutes=1)))
        api_start_utc = self._get_utc_datetime(api_activity.begin)
        api_end_utc = self._get_utc_datetime(api_activity.einde)
        
        if not all([xml_start_utc, xml_end_utc, api_start_utc, api_end_utc]):
            return 0.0, 'Missing converted UTC data'
        
        # Check start time proximity (±5 minutes)
        start_diff_seconds = abs((xml_start_utc - api_start_utc).total_seconds())
        start_close = start_diff_seconds <= self.config.matching.time_start_proximity_tolerance_seconds
        
        # Check for time overlap with buffer
        buffer_seconds = self.config.matching.time_general_overlap_buffer_seconds
        overlap = (max(xml_start_utc, api_start_utc - timedelta(seconds=buffer_seconds)) < 
                  min(xml_end_utc, api_end_utc + timedelta(seconds=buffer_seconds)))
        
        if start_close:
            score = self.config.matching.score_time_start_proximity
            reason = f'Start times close ({start_diff_seconds/60:.1f} min apart)'
            if overlap:
                reason += ' & overlap'
            return score, reason
        elif overlap:
            return self.config.matching.score_time_overlap_only, 'Timeframes overlap'
        
        return 0.0, 'No significant time match'
    
    def _evaluate_soort_match(self, xml_soort: str, api_activity: Activiteit) -> Tuple[float, str]:
        """Evaluate soort (type) matching"""
        
        if not xml_soort:
            return 0.0, 'No XML soort'
        
        xml_s = xml_soort.lower()
        api_s = (
            api_activity.soort.value.lower()
            if api_activity.soort and hasattr(api_activity.soort, 'value')
            else str(api_activity.soort).lower()
        )
        
        if not api_s:
            return 0.0, 'No API soort'
        
        # Exact match
        if xml_s == api_s:
            return self.config.matching.score_soort_exact, "Soort exact match"
        
        # Partial matches
        if xml_s in api_s:
            return self.config.matching.score_soort_partial_xml_in_api, "Soort partial XML in API"
        
        if api_s in xml_s:
            return self.config.matching.score_soort_partial_api_in_xml, "Soort partial API in XML"
        
        # Check aliases
        soort_aliases = {
            'opening': ['aanvang', 'regeling van werkzaamheden', 'reglementair'],
            'sluiting': ['einde vergadering', 'stemmingen', 'stemmen'],
            'mededelingen': ['procedurevergadering', 'procedures en brieven', 'uitstel brieven'],
        }
        
        for main_soort, aliases in soort_aliases.items():
            if xml_s == main_soort:
                for alias in aliases:
                    if alias in api_s:
                        return self.config.matching.score_soort_partial_xml_in_api, f"Soort alias match ('{alias}')"
        
        return 0.0, 'No soort match'
    
    def _evaluate_topic_match(self, xml_activity: XmlActivity, api_activity: Activiteit) -> Tuple[float, List[str]]:
        """Evaluate topic/onderwerp matching with normalization"""
        
        score = 0.0
        reasons = []
        
        api_onderwerp = (api_activity.onderwerp or '').lower()
        xml_onderwerp = (xml_activity.onderwerp or '').lower()
        xml_titel = (xml_activity.titel or '').lower()
        
        # Normalize topics for fair comparison
        norm_api_ond = self.topic_normalizer.normalize(api_onderwerp)
        norm_xml_ond = self.topic_normalizer.normalize(xml_onderwerp)
        norm_xml_tit = self.topic_normalizer.normalize(xml_titel)
        
        # XML onderwerp vs API onderwerp
        if xml_onderwerp and api_onderwerp:
            if norm_xml_ond == norm_api_ond:
                score += self.config.matching.score_onderwerp_exact
                reasons.append("Onderwerp exact match")
            else:
                ratio = fuzz.ratio(norm_xml_ond, norm_api_ond)
                if ratio >= self.config.matching.fuzzy_similarity_threshold_high:
                    score += self.config.matching.score_onderwerp_fuzzy_high
                    reasons.append(f"Onderwerp fuzzy high ({ratio}%)")
                elif ratio >= self.config.matching.fuzzy_similarity_threshold_medium:
                    score += self.config.matching.score_onderwerp_fuzzy_medium
                    reasons.append(f"Onderwerp fuzzy medium ({ratio}%)")
        
        # XML titel vs API onderwerp
        if xml_titel and api_onderwerp:
            if norm_xml_tit == norm_api_ond:
                score += self.config.matching.score_titel_exact_vs_api_onderwerp
                reasons.append("Titel exact vs API onderwerp")
            else:
                ratio = fuzz.ratio(norm_xml_tit, norm_api_ond)
                if ratio >= self.config.matching.fuzzy_similarity_threshold_high:
                    score += self.config.matching.score_titel_fuzzy_high_vs_api_onderwerp
                    reasons.append(f"Titel fuzzy high vs onderwerp ({ratio}%)")
                elif ratio >= self.config.matching.fuzzy_similarity_threshold_medium:
                    score += self.config.matching.score_titel_fuzzy_medium_vs_api_onderwerp
                    reasons.append(f"Titel fuzzy medium vs onderwerp ({ratio}%)")
        
        return score, reasons
    
    def _should_accept_match(self, best_score: float, potential_matches: List[Dict[str, Any]]) -> bool:
        """Determine if we should accept the best match"""
        
        # Accept if score exceeds threshold
        if best_score >= self.config.matching.min_match_score_for_activiteit:
            return True
        
        # Accept if significantly better than runner-up
        if len(potential_matches) > 1:
            runner_up_score = potential_matches[1]['score']
            if best_score - runner_up_score >= 1.0 and best_score >= 1.0:
                return True
        
        return False
    
    def _get_utc_datetime(self, dt_obj: datetime) -> datetime:
        """Convert datetime to UTC"""
        if not dt_obj:
            return None
        
        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
            # Assume local time, convert to UTC
            return (dt_obj - timedelta(hours=self.config.time.local_timezone_offset_hours)).replace(tzinfo=timezone.utc)
        
        return dt_obj.astimezone(timezone.utc) 