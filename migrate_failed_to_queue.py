import sys
import logging
from datetime import datetime, timezone
sys.path.append('.')
from src.core.firestore_client import get_firestore_client
from google.cloud import firestore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MigrateToQueue")

def migrate():
    db = get_firestore_client()
    col_ref = db.collection("bounty_leads")
    
    docs = list(col_ref.where("status", "==", "failed_verification").stream())
    logger.info(f"Found {len(docs)} failed_verification leads.")
    
    migrated = 0
    batch = db.batch()
    
    for idx, doc in enumerate(docs):
        batch.update(doc.reference, {
            "status": "queued",
            "updated_at_iso": datetime.now(timezone.utc).isoformat()
        })
        migrated += 1
        
        if migrated % 400 == 0:
            batch.commit()
            logger.info(f"Committed {migrated} docs...")
            batch = db.batch()
            
    if migrated % 400 != 0:
        batch.commit()
        
    logger.info(f"Successfully migrated {migrated} leads to 'queued'.")

if __name__ == "__main__":
    migrate()
