"""
XML Extractor for VLOS Processing

Pure data extraction from VLOS XML files without any matching or processing logic.
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional, Dict, Any
import re

from ..models import (
    XmlVergadering, XmlActivity, XmlSpeaker, XmlZaak, XmlVotingEvent
)
from ..config import VlosConfig


class XmlExtractor:
    """Extracts structured data from VLOS XML files"""
    
    def __init__(self, config: VlosConfig):
        self.config = config
        self.ns = config.xml_namespace
    
    def extract_vergadering(self, xml_content: str) -> XmlVergadering:
        """Extract vergadering information from XML"""
        root = ET.fromstring(xml_content)
        vergadering_el = root.find('vlos:vergadering', self.ns)
        
        if vergadering_el is None:
            raise ValueError("No vergadering element found in XML")
        
        return XmlVergadering(
            object_id=vergadering_el.get('objectid', 'unknown'),
            soort=vergadering_el.get('soort', ''),
            titel=vergadering_el.findtext('vlos:titel', default='', namespaces=self.ns),
            nummer=vergadering_el.findtext('vlos:vergaderingnummer', default=None, namespaces=self.ns),
            datum=self._parse_datetime(
                vergadering_el.findtext('vlos:datum', default='', namespaces=self.ns)
            ),
            raw_xml=vergadering_el
        )
    
    def extract_activities(self, xml_content: str) -> List[XmlActivity]:
        """Extract all activities from XML"""
        root = ET.fromstring(xml_content)
        vergadering_el = root.find('vlos:vergadering', self.ns)
        
        if vergadering_el is None:
            print("❌ No vergadering element found in XML")
            return []
        
        activities = []
        all_activity_elements = vergadering_el.findall('vlos:activiteit', self.ns)
        
        print(f"🔍 Found {len(all_activity_elements)} total activiteit elements in XML")
        
        filtered_count = 0
        processed_count = 0
        
        for i, xml_act in enumerate(all_activity_elements, 1):
            soort = xml_act.get('soort', '').lower()
            titel = xml_act.findtext('vlos:titel', default='', namespaces=self.ns).lower()
            
            print(f"   Activity {i}: soort='{xml_act.get('soort', '')}', titel='{xml_act.findtext('vlos:titel', default='', namespaces=self.ns)[:50]}...'")
            
            # Skip procedural activities if configured
            should_skip = False
            skip_reason = ""
            
            if self.config.processing.skip_procedural_activities:
                if soort in self.config.processing.procedural_activity_types:
                    should_skip = True
                    skip_reason = f"soort '{soort}' in procedural types"
                elif any(proc_type in titel for proc_type in self.config.processing.procedural_activity_types):
                    should_skip = True
                    skip_reason = f"titel contains procedural keyword"
            
            if should_skip:
                print(f"      ⏭️ SKIPPED: {skip_reason}")
                filtered_count += 1
                continue
            
            print(f"      ✅ PROCESSING activity")
            processed_count += 1
            
            activity = XmlActivity(
                object_id=xml_act.get('objectid', f"activity_{hash(ET.tostring(xml_act))}"),
                soort=xml_act.get('soort', ''),
                titel=xml_act.findtext('vlos:titel', default='', namespaces=self.ns),
                onderwerp=xml_act.findtext('vlos:onderwerp', default='', namespaces=self.ns),
                start_time=self._parse_datetime(
                    xml_act.findtext('vlos:aanvangstijd', default=None, namespaces=self.ns) or
                    xml_act.findtext('vlos:markeertijdbegin', default=None, namespaces=self.ns)
                ),
                end_time=self._parse_datetime(
                    xml_act.findtext('vlos:eindtijd', default=None, namespaces=self.ns) or
                    xml_act.findtext('vlos:markeertijdeind', default=None, namespaces=self.ns)
                ),
                raw_xml=xml_act
            )
            activities.append(activity)
        
        print(f"📊 Activity Extraction Summary:")
        print(f"   Total found: {len(all_activity_elements)}")
        print(f"   Filtered out: {filtered_count}")
        print(f"   Processed: {processed_count}")
        print(f"   Final count: {len(activities)}")
        
        # Check for nested activities that might be missed
        all_nested_activities = root.findall(".//vlos:activiteit", self.ns)
        if len(all_nested_activities) > len(all_activity_elements):
            print(f"⚠️  WARNING: Found {len(all_nested_activities)} total activiteit elements (including nested), but only processed {len(all_activity_elements)} direct children")
            print(f"   There may be {len(all_nested_activities) - len(all_activity_elements)} nested activities being missed!")
        
        return activities
    
    def extract_speakers_from_activity(self, activity: XmlActivity) -> List[XmlSpeaker]:
        """Extract all speakers from a specific activity"""
        speakers = []
        fragment_count = 0
        seen_speakers = set()  # Track unique speakers to avoid duplicates
        
        # Method 1: Extract speakers from draadboekfragment elements (with speech text)
        for frag in activity.raw_xml.findall(".//vlos:draadboekfragment", self.ns):
            tekst_el = frag.find("vlos:tekst", self.ns)
            if tekst_el is None:
                continue
            
            fragment_count += 1
            speech_text = self._collapse_text(tekst_el)
            if not speech_text:
                continue
            
            for sprek_el in frag.findall("vlos:sprekers/vlos:spreker", self.ns):
                voornaam = sprek_el.findtext("vlos:voornaam", default="", namespaces=self.ns)
                achternaam = (
                    sprek_el.findtext("vlos:verslagnaam", default="", namespaces=self.ns) or
                    sprek_el.findtext("vlos:achternaam", default="", namespaces=self.ns)
                )
                verslagnaam = sprek_el.findtext("vlos:verslagnaam", default=None, namespaces=self.ns)
                fractie = sprek_el.findtext("vlos:fractie", default=None, namespaces=self.ns)
                
                if achternaam:  # Only process if we have a last name
                    # Create unique identifier to avoid duplicates
                    speaker_key = f"{voornaam}|{achternaam}|{fractie or 'none'}"
                    if speaker_key not in seen_speakers:
                        seen_speakers.add(speaker_key)
                        speaker = XmlSpeaker(
                            voornaam=voornaam,
                            achternaam=achternaam,
                            verslagnaam=verslagnaam,
                            fractie=fractie,
                            speech_text=speech_text,
                            fragment_id=f"{activity.object_id}_frag_{fragment_count}",
                            raw_xml=sprek_el
                        )
                        speakers.append(speaker)
        
        # Method 2: Extract speakers from all other speaker elements in activity
        speaker_element_count = 0
        for sprek_el in activity.raw_xml.findall(".//vlos:spreker", self.ns):
            voornaam = sprek_el.findtext("vlos:voornaam", default="", namespaces=self.ns)
            achternaam = (
                sprek_el.findtext("vlos:verslagnaam", default="", namespaces=self.ns) or
                sprek_el.findtext("vlos:achternaam", default="", namespaces=self.ns)
            )
            verslagnaam = sprek_el.findtext("vlos:verslagnaam", default=None, namespaces=self.ns)
            fractie = sprek_el.findtext("vlos:fractie", default=None, namespaces=self.ns)
            
            if achternaam:  # Only process if we have a last name
                # Create unique identifier to avoid duplicates
                speaker_key = f"{voornaam}|{achternaam}|{fractie or 'none'}"
                if speaker_key not in seen_speakers:
                    seen_speakers.add(speaker_key)
                    speaker_element_count += 1
                    
                    # Try to find associated speech text from parent elements
                    speech_text = self._extract_speech_text_for_speaker(sprek_el)
                    
                    speaker = XmlSpeaker(
                        voornaam=voornaam,
                        achternaam=achternaam,
                        verslagnaam=verslagnaam,
                        fractie=fractie,
                        speech_text=speech_text,
                        fragment_id=f"{activity.object_id}_speaker_{speaker_element_count}",
                        raw_xml=sprek_el
                    )
                    speakers.append(speaker)
        
        return speakers
    
    def extract_zaken_from_activity(self, activity: XmlActivity) -> List[XmlZaak]:
        """Extract all zaken from a specific activity"""
        zaken = []
        
        for xml_zaak in activity.raw_xml.findall(".//vlos:zaak", self.ns):
            dossiernr = xml_zaak.findtext("vlos:dossiernummer", default="", namespaces=self.ns).strip()
            stuknr = xml_zaak.findtext("vlos:stuknummer", default="", namespaces=self.ns).strip()
            titel = xml_zaak.findtext("vlos:titel", default="", namespaces=self.ns).strip()
            parlisid = xml_zaak.findtext("vlos:parlisid", default="", namespaces=self.ns).strip()
            objectid = xml_zaak.get("objectid", "").strip()
            soort = xml_zaak.get("soort", "").strip()
            
            # Only include zaken with proper dossier/stuk numbers for now
            # We'll get other zaken through Agendapunt connections
            if dossiernr and stuknr:
                zaak = XmlZaak(
                    dossiernummer=dossiernr,
                    stuknummer=stuknr,
                    titel=titel,
                    raw_xml=xml_zaak
                )
                zaken.append(zaak)
        
        return zaken
    
    def extract_voting_from_activity(self, activity: XmlActivity) -> List[XmlVotingEvent]:
        """Extract voting events from a specific activity"""
        voting_events = []
        
        for item in activity.raw_xml.findall(".//vlos:activiteititem", self.ns):
            soort = item.get('soort', '')
            
            # Check for voting-related activity types
            if soort.lower() in ['besluit', 'stemming', 'vote']:
                titel = item.findtext("vlos:titel", default="", namespaces=self.ns)
                besluitvorm = item.findtext("vlos:besluitvorm", default="", namespaces=self.ns)
                uitslag = item.findtext("vlos:uitslag", default="", namespaces=self.ns)
                
                # Extract individual fractie votes
                fractie_votes = []
                stemmingen_el = item.find("vlos:stemmingen", self.ns)
                if stemmingen_el is not None:
                    for stemming in stemmingen_el.findall("vlos:stemming", self.ns):
                        fractie_name = stemming.findtext("vlos:fractie", default="", namespaces=self.ns)
                        stem_value = stemming.findtext("vlos:stem", default="", namespaces=self.ns)
                        
                        if fractie_name and stem_value:
                            fractie_votes.append({
                                'fractie': fractie_name,
                                'vote': stem_value,
                                'vote_normalized': stem_value.lower()
                            })
                
                if fractie_votes:
                    voting_event = XmlVotingEvent(
                        titel=titel,
                        besluitvorm=besluitvorm,
                        uitslag=uitslag,
                        fractie_votes=fractie_votes,
                        raw_xml=item
                    )
                    voting_events.append(voting_event)
        
        return voting_events
    
    def extract_speakers_from_zaak(self, zaak: XmlZaak) -> List[XmlSpeaker]:
        """Extract speakers directly linked to a zaak element"""
        speakers = []
        
        for sprek_el in zaak.raw_xml.findall("vlos:sprekers/vlos:spreker", self.ns):
            voornaam = sprek_el.findtext("vlos:voornaam", default="", namespaces=self.ns)
            achternaam = (
                sprek_el.findtext("vlos:verslagnaam", default="", namespaces=self.ns) or
                sprek_el.findtext("vlos:achternaam", default="", namespaces=self.ns)
            )
            verslagnaam = sprek_el.findtext("vlos:verslagnaam", default=None, namespaces=self.ns)
            fractie = sprek_el.findtext("vlos:fractie", default=None, namespaces=self.ns)
            
            if achternaam:
                speaker = XmlSpeaker(
                    voornaam=voornaam,
                    achternaam=achternaam,
                    verslagnaam=verslagnaam,
                    fractie=fractie,
                    speech_text="[Direct zaak speaker link - no speech text]",
                    fragment_id=f"zaak_{zaak.dossiernummer}_{zaak.stuknummer}",
                    raw_xml=sprek_el
                )
                speakers.append(speaker)
        
        return speakers
    
    def _parse_datetime(self, datetime_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from XML"""
        if not datetime_str or not isinstance(datetime_str, str):
            return None
        
        dt_str = datetime_str.strip()
        try:
            if dt_str.endswith('Z'):
                return datetime.fromisoformat(dt_str[:-1] + '+00:00')
            if len(dt_str) >= 24 and (dt_str[19] in '+-') and dt_str[22] == ':':
                return datetime.fromisoformat(dt_str)
            if len(dt_str) >= 23 and (dt_str[19] in '+-') and dt_str[22] != ':':
                return datetime.fromisoformat(dt_str[:22] + ':' + dt_str[22:])
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    
    def _extract_speech_text_for_speaker(self, sprek_el: ET.Element) -> str:
        """Extract speech text associated with a speaker element - simplified version"""
        # For speakers not in draadboekfragment, we'll use a generic placeholder
        # since reliable parent traversal is complex with ElementTree
        return "[Speaker element - speech text not in draadboekfragment]"

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
        full_text = re.sub(r'\s+', ' ', full_text)
        return full_text.strip() 