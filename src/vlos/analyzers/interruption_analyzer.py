"""
Interruption Analyzer for VLOS Processing

Detects and analyzes parliamentary interruption patterns to understand
debate dynamics and speaker interactions.
"""

from typing import List, Dict, Any
from collections import defaultdict
import xml.etree.ElementTree as ET

from ..config import VlosConfig
from ..models import (
    XmlActivity, SpeakerMatch, InterruptionEvent, InterruptionType, 
    InterruptionAnalysis, ZaakMatch
)


class InterruptionAnalyzer:
    """Analyzes parliamentary interruption patterns"""
    
    def __init__(self, config: VlosConfig):
        self.config = config
        self.ns = config.xml_namespace
    
    def detect_interruptions_in_activity(self, xml_activity: XmlActivity, 
                                        activity_speakers: List[SpeakerMatch],
                                        activity_zaken: List[ZaakMatch],
                                        activity_id: str) -> List[InterruptionEvent]:
        """Detect interruption patterns within an activity"""
        
        if not self.config.analysis.detect_fragment_interruptions and not self.config.analysis.detect_sequential_interruptions:
            return []
        
        interruptions = []
        speaker_sequence = []
        fragment_count = 0
        
        # Track speaker sequence within draadboekfragments
        for frag in xml_activity.raw_xml.findall(".//vlos:draadboekfragment", self.ns):
            tekst_el = frag.find("vlos:tekst", self.ns)
            if tekst_el is None:
                continue
            
            fragment_count += 1
            speech_text = self._collapse_text(tekst_el)
            if not speech_text:
                continue
            
            # Get all speakers in this fragment
            fragment_speakers = []
            for sprek_el in frag.findall("vlos:sprekers/vlos:spreker", self.ns):
                v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=self.ns)
                v_last = (
                    sprek_el.findtext("vlos:verslagnaam", default="", namespaces=self.ns) or
                    sprek_el.findtext("vlos:achternaam", default="", namespaces=self.ns)
                )
                
                if v_last:
                    # Find matching persoon from activity speakers
                    matched_speaker = self._find_matching_speaker(v_first, v_last, activity_speakers)
                    
                    speaker_entry = {
                        'fragment_id': f"frag_{fragment_count}",
                        'speaker_match': matched_speaker,
                        'speech_text': speech_text,
                        'speech_length': len(speech_text)
                    }
                    fragment_speakers.append(speaker_entry)
                    speaker_sequence.append(speaker_entry)
            
            # Detect fragment interruptions (multiple speakers in one fragment)
            if self.config.analysis.detect_fragment_interruptions and len(fragment_speakers) > 1:
                for i in range(1, len(fragment_speakers)):
                    if (fragment_speakers[0]['speaker_match'] and 
                        fragment_speakers[i]['speaker_match'] and
                        fragment_speakers[0]['speaker_match'].persoon_id != fragment_speakers[i]['speaker_match'].persoon_id):
                        
                        interruption = InterruptionEvent(
                            type=InterruptionType.FRAGMENT_INTERRUPTION,
                            original_speaker=fragment_speakers[0]['speaker_match'],
                            interrupting_speaker=fragment_speakers[i]['speaker_match'],
                            activity_id=activity_id,
                            fragment_id=speaker_entry['fragment_id'],
                            context=f"Multiple speakers in fragment {fragment_count}",
                            speech_context=speech_text[:150],
                            topics_discussed=[z.xml_zaak.titel for z in activity_zaken if z.match_result.success],
                            interruption_length=fragment_speakers[i]['speech_length']
                        )
                        interruptions.append(interruption)
        
        # Detect sequential interruptions (A→B→A patterns)
        if self.config.analysis.detect_sequential_interruptions and len(speaker_sequence) >= 3:
            interruptions.extend(self._detect_sequential_interruptions(
                speaker_sequence, activity_id, activity_zaken
            ))
        
        return interruptions
    
    def analyze_interruption_patterns(self, all_interruptions: List[InterruptionEvent]) -> InterruptionAnalysis:
        """Analyze interruption patterns to identify trends and key players"""
        
        if not all_interruptions:
            return InterruptionAnalysis(
                total_interruptions=0,
                interruption_types={},
                most_frequent_interrupters={},
                most_interrupted_speakers={},
                interruption_pairs={},
                topics_causing_interruptions={},
                response_patterns={}
            )
        
        # Count interruption types
        interruption_types = defaultdict(int)
        for interruption in all_interruptions:
            interruption_types[interruption.type.value] += 1
        
        # Track who interrupts whom
        interruption_pairs = defaultdict(lambda: {
            'count': 0,
            'interrupter': '',
            'interrupted': '',
            'topics': set(),
            'examples': []
        })
        
        interrupter_counts = defaultdict(int)
        interrupted_counts = defaultdict(int)
        
        for interruption in all_interruptions:
            if (interruption.interrupting_speaker.persoon_id and 
                interruption.original_speaker.persoon_id):
                
                interrupter = interruption.interrupting_speaker.persoon_name or "Unknown"
                interrupted = interruption.original_speaker.persoon_name or "Unknown"
                pair_key = f"{interrupter} → {interrupted}"
                
                interruption_pairs[pair_key]['count'] += 1
                interruption_pairs[pair_key]['interrupter'] = interrupter
                interruption_pairs[pair_key]['interrupted'] = interrupted
                interruption_pairs[pair_key]['topics'].update(interruption.topics_discussed)
                interruption_pairs[pair_key]['examples'].append(interruption)
                
                interrupter_counts[interrupter] += 1
                interrupted_counts[interrupted] += 1
        
        # Topics causing interruptions
        topic_interruption_counts = defaultdict(lambda: {
            'count': 0,
            'interruption_events': []
        })
        
        for interruption in all_interruptions:
            for topic in interruption.topics_discussed:
                topic_interruption_counts[topic]['count'] += 1
                topic_interruption_counts[topic]['interruption_events'].append(interruption)
        
        # Response patterns
        response_patterns = defaultdict(lambda: {
            'count': 0,
            'responder': '',
            'interrupter': '',
            'topics': set()
        })
        
        if self.config.analysis.detect_response_patterns:
            for interruption in all_interruptions:
                if (interruption.type == InterruptionType.INTERRUPTION_WITH_RESPONSE and
                    interruption.responding_speaker):
                    
                    responder = interruption.responding_speaker.persoon_name or "Unknown"
                    interrupter = interruption.interrupting_speaker.persoon_name or "Unknown"
                    response_key = f"{responder} responds to {interrupter}"
                    
                    response_patterns[response_key]['count'] += 1
                    response_patterns[response_key]['responder'] = responder
                    response_patterns[response_key]['interrupter'] = interrupter
                    response_patterns[response_key]['topics'].update(interruption.topics_discussed)
        
        return InterruptionAnalysis(
            total_interruptions=len(all_interruptions),
            interruption_types=dict(interruption_types),
            most_frequent_interrupters=dict(sorted(interrupter_counts.items(), key=lambda x: x[1], reverse=True)),
            most_interrupted_speakers=dict(sorted(interrupted_counts.items(), key=lambda x: x[1], reverse=True)),
            interruption_pairs=dict(sorted(interruption_pairs.items(), key=lambda x: x[1]['count'], reverse=True)),
            topics_causing_interruptions=dict(sorted(topic_interruption_counts.items(), key=lambda x: x[1]['count'], reverse=True)),
            response_patterns=dict(sorted(response_patterns.items(), key=lambda x: x[1]['count'], reverse=True))
        )
    
    def _detect_sequential_interruptions(self, speaker_sequence: List[Dict], 
                                       activity_id: str, activity_zaken: List[ZaakMatch]) -> List[InterruptionEvent]:
        """Detect A→B→A interruption patterns in speaker sequence"""
        
        interruptions = []
        
        for i in range(1, len(speaker_sequence) - 1):
            current = speaker_sequence[i]
            prev_speaker = speaker_sequence[i-1]
            next_speaker = speaker_sequence[i+1] if i+1 < len(speaker_sequence) else None
            
            # Pattern: A speaks, B interrupts, A responds
            if (prev_speaker['speaker_match'] and current['speaker_match'] and
                prev_speaker['speaker_match'].persoon_id != current['speaker_match'].persoon_id):
                
                # Check if previous speaker returns (indicating a response to interruption)
                if (next_speaker and next_speaker['speaker_match'] and
                    next_speaker['speaker_match'].persoon_id == prev_speaker['speaker_match'].persoon_id):
                    
                    interruption = InterruptionEvent(
                        type=InterruptionType.INTERRUPTION_WITH_RESPONSE,
                        original_speaker=prev_speaker['speaker_match'],
                        interrupting_speaker=current['speaker_match'],
                        responding_speaker=next_speaker['speaker_match'],
                        activity_id=activity_id,
                        fragment_id=current['fragment_id'],
                        context=f"{prev_speaker['speaker_match'].persoon_name} interrupted by {current['speaker_match'].persoon_name}, then responds",
                        speech_context=current['speech_text'][:150],
                        topics_discussed=[z.xml_zaak.titel for z in activity_zaken if z.match_result.success],
                        interruption_length=current['speech_length']
                    )
                    interruptions.append(interruption)
                else:
                    # Simple interruption without clear response
                    interruption = InterruptionEvent(
                        type=InterruptionType.SIMPLE_INTERRUPTION,
                        original_speaker=prev_speaker['speaker_match'],
                        interrupting_speaker=current['speaker_match'],
                        activity_id=activity_id,
                        fragment_id=current['fragment_id'],
                        context=f"{prev_speaker['speaker_match'].persoon_name} interrupted by {current['speaker_match'].persoon_name}",
                        speech_context=current['speech_text'][:150],
                        topics_discussed=[z.xml_zaak.titel for z in activity_zaken if z.match_result.success],
                        interruption_length=current['speech_length']
                    )
                    interruptions.append(interruption)
        
        return interruptions
    
    def _find_matching_speaker(self, first_name: str, last_name: str, 
                             activity_speakers: List[SpeakerMatch]) -> SpeakerMatch:
        """Find matching speaker from activity speakers list"""
        
        for speaker_match in activity_speakers:
            if (speaker_match.xml_speaker.achternaam.lower() == last_name.lower() or
                (speaker_match.persoon_name and 
                 last_name.lower() in speaker_match.persoon_name.lower())):
                return speaker_match
        
        return None
    
    def _collapse_text(self, element: ET.Element) -> str:
        """Collapse XML text content from an element and all its children"""
        def _extract_text(elem):
            text_parts = []
            if elem.text:
                text_parts.append(elem.text.strip())
            for child in elem:
                text_parts.extend(_extract_text(child))
                if child.tail:
                    text_parts.append(child.tail.strip())
            return text_parts
        
        text_parts = _extract_text(element)
        full_text = ' '.join(part for part in text_parts if part)
        
        # Clean up whitespace
        import re
        full_text = re.sub(r'\s+', ' ', full_text)
        return full_text.strip() 