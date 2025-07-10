"""
VLOS Speaker to Persoon matching logic
"""
from typing import Optional, List, Dict, Any, Tuple
from thefuzz import fuzz
from utils.helpers import merge_rel


def normalize_name(name: str) -> str:
    """Normalize a name for comparison"""
    if not name:
        return ""
    
    # Remove common prefixes
    prefixes_to_remove = [
        "de heer ", "mevrouw ",
        "minister ", "staatssecretaris ", "minister-president ",
        "minister van ", "minister voor ",
        "de ", "het ", "van ", "der ", "den "
    ]
    normalized = name.lower().strip()
    
    for prefix in prefixes_to_remove:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
    
    return normalized.strip()

# -------------------------------------------------
# New helpers for stricter matching
# -------------------------------------------------


def _detect_gender_from_title(raw_name: str) -> str | None:
    """Infer gender from Dutch parliamentary salutation prefix.

    Returns "man", "vrouw", or None when it cannot be inferred."""
    if not raw_name:
        return None
    lower = raw_name.lower().strip()
    if lower.startswith("mevrouw"):
        return "vrouw"
    if lower.startswith("de heer"):
        return "man"
    return None


def _first_name_close_match(a: str, b: str) -> bool:
    """Return True when first names are the same or highly similar (>= 80 fuzzy)."""
    if not a or not b:
        return False
    a_l, b_l = a.lower().strip(), b.lower().strip()
    if a_l == b_l:
        return True
    return fuzz.ratio(a_l, b_l) >= 80


# -----------------------------
# Name parsing improvements
# -----------------------------

def _strip_title_prefix(name: str) -> str:
    """Remove common titles (Minister, Staatssecretaris, etc.) and salutations"""
    lower = name.lower().strip()
    titles = [
        "minister-president", "minister van", "minister voor", "minister",
        "staatssecretaris", "de heer", "mevrouw"
    ]
    for t in titles:
        if lower.startswith(t):
            return name[len(t):].strip()
    return name.strip()


def _reorder_dutch_prefix(tokens: list[str]) -> list[str]:
    """If tokens end with a surname prefix (van, de, der, den), move it before previous token(s).
    E.g. ["Rij", "van"] -> ["van", "Rij"]"""
    if len(tokens) < 2:
        return tokens

    last = tokens[-1].lower()
    second_last = tokens[-2].lower() if len(tokens) >= 2 else ''

    # Single-word prefix at end (… van / de / der / den)
    if last in {"van", "de", "der", "den"}:
        return [tokens[-1]] + tokens[:-1]

    # Two-word Dutch prefixes such as "van der", "van de", "van den"
    if second_last == "van" and last in {"der", "de", "den"}:
        return [tokens[-2], tokens[-1]] + tokens[:-2]

    return tokens


def extract_name_parts(vlos_name: str) -> Dict[str, str]:
    """Extract name parts from VLOS speaker name"""
    clean_name = _strip_title_prefix(vlos_name)
    
    # Split into parts
    parts = clean_name.strip().split()

    # Reorder Dutch prefix if necessary (e.g. "Rij van" -> "van Rij")
    parts = _reorder_dutch_prefix(parts)
    
    if len(parts) == 0:
        return {"voornaam": "", "achternaam": ""}
    elif len(parts) == 1:
        return {"voornaam": "", "achternaam": parts[0]}
    else:
        # First part is usually voornaam, rest is achternaam (including tussenvoegsel)
        return {
            "voornaam": parts[0],
            "achternaam": " ".join(parts[1:])
        }


def calculate_name_similarity(vlos_speaker: Dict[str, Any], persoon: Dict[str, Any]) -> float:
    """Calculate name similarity between VLOS speaker and Persoon"""
    score = 0.0
    max_score = 100.0
    
    # Extract VLOS name parts
    vlos_voornaam = str(vlos_speaker.get('voornaam', '') or '').strip()
    vlos_achternaam = str(vlos_speaker.get('achternaam', '') or '').strip()
    
    # Get Persoon name parts (handle None values from database)
    persoon_roepnaam = str(persoon.get('roepnaam', '') or '').strip()
    persoon_voornaam = str(persoon.get('voornaam', '') or '').strip()
    persoon_achternaam = str(persoon.get('achternaam', '') or '').strip()
    persoon_tussenvoegsel = str(persoon.get('tussenvoegsel', '') or '').strip()
    
    # Build full achternaam for Persoon (including tussenvoegsel)
    persoon_full_achternaam = f"{persoon_tussenvoegsel} {persoon_achternaam}".strip()
    
    # Voornaam matching (use roepnaam if available, otherwise voornaam)
    persoon_first_name = persoon_roepnaam if persoon_roepnaam else persoon_voornaam
    
    if vlos_voornaam and persoon_first_name:
        if vlos_voornaam.lower() == persoon_first_name.lower():
            score += 40.0  # Exact match
        else:
            fuzzy_score = fuzz.ratio(vlos_voornaam.lower(), persoon_first_name.lower())
            if fuzzy_score >= 80:
                score += 30.0  # High similarity
            elif fuzzy_score >= 60:
                score += 15.0  # Medium similarity
    
    # Achternaam matching
    if vlos_achternaam and persoon_achternaam:
        # Pre-process to handle trailing Dutch prefixes like "Haasen van" → "van Haasen"
        def normalize_dutch_surname(name: str) -> str:
            """Convert 'Wal van der' → 'van der Wal' while keeping multi-word prefixes."""
            parts = name.lower().strip().split()
            if len(parts) >= 2 and parts[-1] in {"van", "de", "der", "den"}:
                # Single-word prefix at end → move it in front
                prefix = parts[-1]
                rest = parts[:-1]
                return " ".join([prefix] + rest)
            if len(parts) >= 3 and parts[-2:] == ["van", "der"]:
                # Two-word prefix "van der" at end
                rest = parts[:-2]
                return "van der " + " ".join(rest)
            return " ".join(parts)

        norm_vlos = normalize_dutch_surname(vlos_achternaam)
        norm_pers_full = normalize_dutch_surname(persoon_full_achternaam)
        norm_pers_simple = normalize_dutch_surname(persoon_achternaam)

        # Try exact match with normalized variants
        if norm_vlos == norm_pers_full:
            score += 60.0
        elif norm_vlos == norm_pers_simple:
            score += 55.0
        else:
            # Fuzzy compare, try both directions just in case
            candidates = [
                fuzz.ratio(norm_vlos, norm_pers_full),
                fuzz.ratio(norm_vlos, norm_pers_simple),
                fuzz.ratio(norm_vlos[::-1], norm_pers_full),  # reversed string unlikely but cheap
            ]
            best_fuzzy = max(candidates)

            if best_fuzzy >= 85:
                score += 45.0  # High similarity
            elif best_fuzzy >= 70:
                score += 25.0  # Medium similarity
            elif best_fuzzy >= 50:
                score += 10.0  # Low similarity
    
    return min(score, max_score)


def get_persoon_fractie_from_relationships(session, persoon_id: str) -> Optional[str]:
    """Get the fractie for a Persoon by looking at their recent voting relationships"""
    query = """
    MATCH (p:Persoon {id: $persoon_id})-[:CAST_BY]-(s:Stemming)-[:REPRESENTS_FRACTIE_VOTE]->(f:Fractie)
    RETURN f.naam as fractie_naam, f.afkorting as fractie_afkorting, count(*) as vote_count
    ORDER BY vote_count DESC
    LIMIT 1
    """
    
    try:
        result = session.run(query, persoon_id=persoon_id)
        record = result.single()
        if record:
            return record['fractie_naam']
    except Exception:
        pass
    
    return None


def validate_fractie_match(vlos_fractie: str, persoon_fractie: str) -> bool:
    """Validate if VLOS fractie matches Persoon fractie"""
    # Handle None values properly
    if not vlos_fractie or not persoon_fractie:
        return False
    
    # Ensure we have strings and strip whitespace
    vlos_fractie = str(vlos_fractie).strip() if vlos_fractie else ''
    persoon_fractie = str(persoon_fractie).strip() if persoon_fractie else ''
    
    # Check again after cleaning
    if not vlos_fractie or not persoon_fractie:
        return False
    
    # Exact match
    if vlos_fractie.lower() == persoon_fractie.lower():
        return True
    
    # Common abbreviation mappings
    fractie_mappings = {
        'groenlinks-pvda': ['groenlinks-pvda', 'gl-pvda'],
        'd66': ['democraten 66', 'd66'],
        'vvd': ['volkspartij voor vrijheid en democratie', 'vvd'],
        'cda': ['christen-democratisch appèl', 'cda'],
        'denk': ['denk'],
        'sgp': ['staatkundig gereformeerde partij', 'sgp'],
        'christenunie': ['christenunie', 'cu'],
        'sp': ['socialistische partij', 'sp'],
        'volt': ['volt'],
        'nsc': ['nieuw sociaal contract', 'nsc']
    }
    
    vlos_lower = vlos_fractie.lower()
    persoon_lower = persoon_fractie.lower()
    
    for key, variants in fractie_mappings.items():
        if vlos_lower in variants and persoon_lower in variants:
            return True
    
    # Fuzzy matching as fallback
    return fuzz.ratio(vlos_lower, persoon_lower) >= 80


def find_matching_persoon(session, vlos_speaker: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Find the best matching Persoon for a VLOS speaker"""
    
    # Extract name parts from VLOS speaker
    name_parts = extract_name_parts(vlos_speaker.get('name', ''))
    vlos_voornaam = vlos_speaker.get('voornaam', name_parts['voornaam']) or ''
    vlos_achternaam = vlos_speaker.get('achternaam', name_parts['achternaam']) or ''
    vlos_fractie = str(vlos_speaker.get('fractie', '') or '').strip()

    # -------------------------------------------------
    # Enforce first-name and gender consistency up-front
    # -------------------------------------------------

    speaker_gender = _detect_gender_from_title(vlos_speaker.get('name', ''))

    # Query for potential Persoon matches (current and former officials)
    query = """
    MATCH (p:Persoon)
    WHERE p.functie CONTAINS 'Kamerlid' 
       OR p.functie CONTAINS 'minister' 
       OR p.functie CONTAINS 'staatssecretaris'
    RETURN 
      p.id as persoon_id,
      p.roepnaam as roepnaam,
      p.voornaam as voornaam,
      p.achternaam as achternaam,
      p.tussenvoegsel as tussenvoegsel,
      p.functie as functie
    """
    
    try:
        result = session.run(query)
        candidates = []
        
        for record in result:
            persoon_data = dict(record)
            
            # Determine roles up-front so we can use them in the checks below
            functie_lower = persoon_data.get('functie', '').lower()
            is_kamerlid = 'kamerlid' in functie_lower or 'lid tweede kamer' in functie_lower
            is_minister = 'minister' in persoon_data.get('functie', '').lower() or 'staatssecretaris' in persoon_data.get('functie', '').lower()

            # Calculate name similarity
            name_score = calculate_name_similarity(
                {'voornaam': vlos_voornaam, 'achternaam': vlos_achternaam},
                persoon_data
            )

            # Hard filter: first name must be a close match (protect against Pierre≠Vivianne etc.)
            persoon_first_name_raw = persoon_data.get('roepnaam') or persoon_data.get('voornaam') or ''
            if not _first_name_close_match(vlos_voornaam, persoon_first_name_raw):
                continue  # skip – first names too different

            # Hard filter on gender if we can infer it
            if speaker_gender and persoon_data.get('geslacht'):
                if speaker_gender != persoon_data['geslacht'].lower():
                    continue  # skip – gender mismatch

            # Extra guard: for ministers/state secretaries require strong first-name match
            if is_minister and name_score < 60:
                continue  # Too weak, skip early

            # Skip if name similarity is too low overall
            if name_score < 30.0:
                continue

            # Get fractie for this Persoon (only for Kamerleden)
            persoon_fractie = None
            
            if is_kamerlid:
                persoon_fractie = get_persoon_fractie_from_relationships(session, persoon_data['persoon_id'])
            
            # Validate fractie match (only for Kamerleden, ministers don't have fractie)
            if is_minister:
                fractie_match = True  # Ministers don't have fractie, so skip this check
            elif vlos_fractie and is_kamerlid:
                fractie_match = validate_fractie_match(vlos_fractie, persoon_fractie)
            else:
                fractie_match = True  # No fractie info to validate
            
            # Calculate total score
            total_score = name_score
            if is_minister and not fractie_match:
                # ministers shouldn't depend on fractie
                pass
            if fractie_match and vlos_fractie and is_kamerlid:
                total_score += 50.0  # Bonus for fractie match (only for Kamerleden)
            elif vlos_fractie and is_kamerlid and not fractie_match:
                total_score -= 30.0  # Penalty for fractie mismatch (only for Kamerleden)
            
            candidates.append({
                'persoon_data': persoon_data,
                'persoon_fractie': persoon_fractie,
                'name_score': name_score,
                'fractie_match': fractie_match,
                'total_score': total_score
            })
        
        # Sort by total score and return best match
        candidates.sort(key=lambda x: x['total_score'], reverse=True)
        
        if candidates and candidates[0]['total_score'] >= 60.0:
            return candidates[0]
    
    except Exception as e:
        print(f"Error finding matching Persoon: {e}")
    
    return None


def match_vlos_speakers_to_personen(session) -> int:
    """Match all VLOS speakers to Persoon nodes and create relationships"""
    
    # Get all VLOS speakers (Kamerleden, ministers, and other officials)
    query = """
    MATCH (vs:VlosSpeaker)
    WHERE vs.functie CONTAINS 'Kamerlid' 
       OR vs.functie CONTAINS 'minister' 
       OR vs.functie CONTAINS 'lid Tweede Kamer'
       OR vs.functie CONTAINS 'staatssecretaris'
    RETURN 
      vs.id as speaker_id,
      vs.name as name,
      vs.voornaam as voornaam,
      vs.achternaam as achternaam,
      vs.functie as functie,
      vs.fractie as fractie
    """
    
    try:
        result = session.run(query)
        vlos_speakers = list(result)
        
        matched_count = 0
        
        for speaker_record in vlos_speakers:
            vlos_speaker = dict(speaker_record)
            
            # Debug: Print speaker data
            print(f"🔍 Processing speaker: {vlos_speaker}")
            
            # Find matching Persoon
            match_result = find_matching_persoon(session, vlos_speaker)
            
            if match_result:
                persoon_data = match_result['persoon_data']
                persoon_fractie = match_result['persoon_fractie']
                
                # Create relationship
                session.execute_write(
                    merge_rel,
                    'VlosSpeaker', 'id', vlos_speaker['speaker_id'],
                    'Persoon', 'id', persoon_data['persoon_id'],
                    'MATCHES_PERSOON'
                )
                
                matched_count += 1
                
                print(f"✅ Matched: {vlos_speaker['name']} -> {persoon_data['roepnaam']} {persoon_data['achternaam']}")
                print(f"   Fractie: {vlos_speaker['fractie']} -> {persoon_fractie}")
                print(f"   Score: {match_result['total_score']:.1f}")
                print()
            else:
                print(f"❌ No match found for: {vlos_speaker['name']} ({vlos_speaker['fractie']})")
        
        return matched_count
    
    except Exception as e:
        print(f"Error in matching process: {e}")
        return 0 