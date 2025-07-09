from tkapi import TKApi
from tkapi.commissie import (
    Commissie,
    CommissieZetel,
    CommissieZetelVastPersoon,
    CommissieZetelVervangerPersoon,
    CommissieZetelVastVacature,
    CommissieZetelVervangerVacature,
)
from tkapi.persoon import Persoon
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry
from core.config.constants import (
    REL_MAP_COMMISSIE, REL_MAP_COMMISSIE_ZETEL, REL_MAP_COMMISSIE_ZETEL_PERSOON,
)
import time

class CommissieLoader(BaseLoader):
    """Loads Commissie and all nested seat/person structures."""
    def __init__(self):
        super().__init__(
            name="commissie_loader",
            description="Loads Commissions (Commissie) from TK API"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.RELATIONSHIP_PROCESSING,
        ]

    def load(self, conn: Neo4jConnection, config: LoaderConfig, checkpoint_manager=None) -> LoaderResult:
        start = time.time()
        res = LoaderResult(
            success=False, processed_count=0, failed_count=0, skipped_count=0,
            total_items=0, execution_time_seconds=0.0, error_messages=[], warnings=[]
        )
        try:
            load_commissies(conn)
            res.success = True
        except Exception as e:
            res.error_messages.append(str(e))
        finally:
            res.execution_time_seconds = time.time() - start
        return res

# Register
commissie_loader_instance = CommissieLoader()
loader_registry.register(commissie_loader_instance)


def load_commissies(conn: Neo4jConnection, batch_size: int | None = None):
    api = TKApi()
    commissies = api.get_items(Commissie, max_items=batch_size) if batch_size else api.get_items(Commissie)
    print(f"→ Fetched {len(commissies)} Commissies")
    with conn.driver.session(database=conn.database) as session:
        for idx, c in enumerate(commissies, 1):
            if idx % 100 == 0 or idx == len(commissies):
                print(f"  → Processing Commissie {idx}/{len(commissies)}")
            # Merge commissie node
            props = {
                'id': c.id,
                'naam': c.naam,
                'afkorting': c.afkorting,
                'soort': c.soort,
                'nummer': c.nummer,
            }
            session.execute_write(merge_node, 'Commissie', 'id', props)

            # First level relationships
            for attr, (target_label, rel_type, key_prop) in REL_MAP_COMMISSIE.items():
                rel_items = getattr(c, attr, []) or []
                if not isinstance(rel_items, (list, tuple)):
                    rel_items = [rel_items]
                for ri in rel_items:
                    if not ri:
                        continue
                    key_val = getattr(ri, key_prop, None)
                    if key_val is None:
                        continue
                    # Minimal props for contactinfo / seat
                    seat_props = {'id': key_val}
                    if target_label == 'CommissieContactinformatie':
                        seat_props.update({'soort': getattr(ri, 'soort', None), 'waarde': getattr(ri, 'waarde', None)})
                    session.execute_write(merge_node, target_label, key_prop, seat_props)
                    session.execute_write(merge_rel, 'Commissie', 'id', c.id, target_label, key_prop, key_val, rel_type)

                    # If seat, process persons/vacancies
                    if target_label == 'CommissieZetel':
                        _process_commissie_zetel(session, ri)

    print("✅ Loaded Commissies and related entities.")


def _process_commissie_zetel(session, zetel_obj: CommissieZetel):
    zetel_id = zetel_obj.id
    # Ensure zetel node already merged by caller
    for attr, (label, rel_type, key_prop) in REL_MAP_COMMISSIE_ZETEL.items():
        items = getattr(zetel_obj, attr, []) or []
        if not isinstance(items, (list, tuple)):
            items = [items]
        for it in items:
            if not it:
                continue
            key_val = getattr(it, key_prop, None)
            if key_val is None:
                continue
            props_it = {'id': key_val}
            if label.endswith('Persoon'):
                props_it.update({
                    'functie': getattr(it, 'functie', None),
                    'van': str(getattr(it, 'van', None)) if getattr(it, 'van', None) else None,
                    'tot_en_met': str(getattr(it, 'tot_en_met', None)) if getattr(it, 'tot_en_met', None) else None,
                })
            elif label.endswith('Vacature'):
                props_it.update({
                    'functie': getattr(it, 'functie', None).name if getattr(it, 'functie', None) else None,
                    'van': str(getattr(it, 'van', None)) if getattr(it, 'van', None) else None,
                    'tot_en_met': str(getattr(it, 'tot_en_met', None)) if getattr(it, 'tot_en_met', None) else None,
                })
            session.execute_write(merge_node, label, key_prop, props_it)
            session.execute_write(merge_rel, 'CommissieZetel', 'id', zetel_id, label, key_prop, key_val, rel_type)

            # If persoon seat, link underlying Person
            if label in ('CommissieZetelVastPersoon', 'CommissieZetelVervangerPersoon'):
                for p_attr, (p_label, p_rel_type, p_key_prop) in REL_MAP_COMMISSIE_ZETEL_PERSOON.items():
                    person_obj = getattr(it, p_attr, None)
                    if not person_obj:
                        continue
                    p_key_val = getattr(person_obj, p_key_prop, None)
                    if p_key_val is None:
                        continue
                    session.execute_write(merge_node, p_label, p_key_prop, {p_key_prop: p_key_val, 'naam': getattr(person_obj, 'naam', None)})
                    session.execute_write(merge_rel, label, key_prop, key_val, p_label, p_key_prop, p_key_val, p_rel_type) 

            # For vacancies, link to the owning Fractie if available
            if label.endswith('Vacature'):
                fractie_obj = getattr(it, 'fractie', None)
                if fractie_obj and getattr(fractie_obj, 'id', None):
                    fractie_id = fractie_obj.id
                    # Ensure Fractie node exists (minimal)
                    session.execute_write(merge_node, 'Fractie', 'id', {'id': fractie_id})
                    # Create the relationship Vacature -> Fractie
                    session.execute_write(merge_rel, label, key_prop, key_val, 'Fractie', 'id', fractie_id, 'BELONGS_TO_FRACTIE') 