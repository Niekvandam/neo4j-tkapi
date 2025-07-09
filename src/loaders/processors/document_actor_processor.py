"""
Processor for DocumentActor – creates links to Persoon, Fractie and Commissie.
"""
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_DOCUMENT_ACTOR

def process_single_document_actor(session, actor_obj, parent_document_id: str):
    """Create DocumentActor node (if not yet present) and link to its related entities.

    Args:
        session: Neo4j session
        actor_obj: DocumentActor object from TK API
        parent_document_id: id of the Document this actor belongs to (so we can create edge)
    """
    if not actor_obj or not actor_obj.id:
        return False

    # Merge the actor node – caller may have done this already, harmless to repeat
    actor_props = {
        'id': actor_obj.id,
        'actor_naam': getattr(actor_obj, 'naam', None),
        'actor_fractie': getattr(actor_obj, 'naam_fractie', None),
        'functie': getattr(actor_obj, 'functie', None),
    }
    session.execute_write(merge_node, 'DocumentActor', 'id', actor_props)

    # Ensure relationship to parent Document exists (Document loader adds it but safe-guard)
    if parent_document_id:
        session.execute_write(merge_rel,
                              'Document', 'id', parent_document_id,
                              'DocumentActor', 'id', actor_obj.id,
                              'HAS_ACTOR')

    # Link to Persoon/Fractie/Commissie via mapping
    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_DOCUMENT_ACTOR.items():
        related_item = getattr(actor_obj, attr_name, None)
        if not related_item:
            continue
        items = related_item if isinstance(related_item, list) else [related_item]
        for it in items:
            key_val = getattr(it, target_key_prop, None)
            if key_val is None:
                continue
            # Merge target node (persoons/fractie/commissie minimal)
            session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: key_val})
            session.execute_write(merge_rel,
                                  'DocumentActor', 'id', actor_obj.id,
                                  target_label, target_key_prop, key_val,
                                  rel_type)
    return True 