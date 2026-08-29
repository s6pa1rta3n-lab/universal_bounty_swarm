"""
Firebase Admin SDK & Google Cloud Firestore Client Module.

Provides Application Default Credentials (ADC) resolution, Firebase Admin app
initialization, Firestore client provisioning, and collection accessors for
the Universal Bounty Swarm.
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, Union, Any, Dict

import google.auth
from google.oauth2 import service_account
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials as fb_credentials

logger = logging.getLogger("UniversalBountySwarm.FirestoreClient")

# Standard / fallback configuration constants
DEFAULT_CREDENTIALS_PATH = "/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json"
DEFAULT_PROJECT_ID = "odin-500008"
DEFAULT_REGION = "us-central1"


def resolve_credentials_path(custom_path: Optional[Union[str, Path]] = None) -> Optional[str]:
    """
    Resolves the Google Cloud service account credentials path.

    Checks:
    1. Explicit custom_path argument
    2. GOOGLE_APPLICATION_CREDENTIALS environment variable
    3. DEFAULT_CREDENTIALS_PATH (/Users/solveetcoagula/Desktop/activeProjects/bounty_operations/.agents/credentials.json)
    4. Relative .agents/credentials.json in cwd or parent directories

    Returns:
        Absolute path to credentials JSON if found, else None.
    """
    candidate_paths = []
    if custom_path:
        candidate_paths.append(Path(custom_path))

    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path:
        candidate_paths.append(Path(env_path))

    candidate_paths.append(Path(DEFAULT_CREDENTIALS_PATH))

    # Also search current directory and parents for .agents/credentials.json
    curr = Path.cwd()
    candidate_paths.append(curr / ".agents" / "credentials.json")
    for parent in curr.parents:
        candidate_paths.append(parent / ".agents" / "credentials.json")

    for p in candidate_paths:
        try:
            resolved = p.expanduser().resolve()
            if resolved.is_file() and resolved.stat().st_size > 0:
                # Set environment variable so underlying Google Auth libraries pick it up
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(resolved)
                return str(resolved)
        except Exception as e:
            logger.debug(f"Error inspecting credential path candidate {p}: {e}")

    return None


def resolve_project_id(
    explicit_project: Optional[str] = None,
    credentials_path: Optional[str] = None
) -> str:
    """
    Resolves the GCP project ID.

    Checks:
    1. Explicit project ID parameter
    2. FIRESTORE_PROJECT_ID / GCP_PROJECT / GOOGLE_CLOUD_PROJECT env vars
    3. project_id inside the credentials JSON
    4. DEFAULT_PROJECT_ID fallback (odin-500008)
    """
    if explicit_project:
        return explicit_project

    for env_var in ["FIRESTORE_PROJECT_ID", "GCP_PROJECT", "GOOGLE_CLOUD_PROJECT"]:
        val = os.getenv(env_var)
        if val:
            return val

    cred_path = credentials_path or resolve_credentials_path()
    if cred_path and os.path.isfile(cred_path):
        try:
            with open(cred_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "project_id" in data:
                    return data["project_id"]
        except Exception as e:
            logger.debug(f"Could not parse project_id from {cred_path}: {e}")

    return DEFAULT_PROJECT_ID


def initialize_firebase_app(
    credentials_path: Optional[str] = None,
    project_id: Optional[str] = None,
    app_name: Optional[str] = None
) -> firebase_admin.App:
    """
    Initializes or retrieves the Firebase Admin App using ADC or service account credentials.

    Args:
        credentials_path: Optional path to service account JSON key.
        project_id: Optional GCP project ID.
        app_name: Optional custom app name (defaults to [DEFAULT]).

    Returns:
        The initialized firebase_admin.App instance.
    """
    cred_file = resolve_credentials_path(credentials_path)
    proj_id = resolve_project_id(project_id, cred_file)

    target_name = app_name or firebase_admin._DEFAULT_APP_NAME

    # Check if app already initialized
    try:
        existing_app = firebase_admin.get_app(name=target_name)
        if existing_app:
            return existing_app
    except ValueError:
        pass  # App does not exist yet, proceed to initialize

    options: Dict[str, Any] = {"projectId": proj_id}

    if cred_file and os.path.isfile(cred_file):
        fb_cred = fb_credentials.Certificate(cred_file)
    else:
        fb_cred = fb_credentials.ApplicationDefault()

    if target_name == firebase_admin._DEFAULT_APP_NAME:
        app = firebase_admin.initialize_app(fb_cred, options=options)
    else:
        app = firebase_admin.initialize_app(fb_cred, options=options, name=target_name)

    logger.info(f"Firebase Admin App '{target_name}' initialized for project '{proj_id}'")
    return app


def get_firestore_client(
    project_id: Optional[str] = None,
    credentials_path: Optional[str] = None,
    database: Optional[str] = None
) -> firestore.Client:
    """
    Returns a configured Google Cloud Firestore Client instance.

    Args:
        project_id: GCP project ID (defaults to resolved odin-500008).
        credentials_path: Path to credentials JSON file.
        database: Optional named Firestore database ID (defaults to '(default)').

    Returns:
        google.cloud.firestore.Client
    """
    cred_file = resolve_credentials_path(credentials_path)
    proj_id = resolve_project_id(project_id, cred_file)

    if cred_file and os.path.isfile(cred_file):
        client_credentials = service_account.Credentials.from_service_account_file(cred_file)
        if database:
            client = firestore.Client(project=proj_id, credentials=client_credentials, database=database)
        else:
            client = firestore.Client(project=proj_id, credentials=client_credentials)
    else:
        if database:
            client = firestore.Client(project=proj_id, database=database)
        else:
            client = firestore.Client(project=proj_id)

    logger.info(f"Firestore Client initialized for project '{proj_id}'")
    return client


# Collection accessors
def get_collection(
    collection_name: str,
    db: Optional[firestore.Client] = None
) -> firestore.CollectionReference:
    """Returns a Firestore CollectionReference."""
    client = db if db is not None else get_firestore_client()
    return client.collection(collection_name)


def get_leads_collection(
    db: Optional[firestore.Client] = None,
    collection_name: str = "bounty_leads"
) -> firestore.CollectionReference:
    """Returns the bounty_leads collection reference."""
    return get_collection(collection_name, db)


def get_operations_collection(
    db: Optional[firestore.Client] = None,
    collection_name: str = "swarm_operations"
) -> firestore.CollectionReference:
    """Returns the swarm_operations collection reference."""
    return get_collection(collection_name, db)


def get_memory_collection(
    db: Optional[firestore.Client] = None,
    collection_name: str = "bounty_memory"
) -> firestore.CollectionReference:
    """Returns the bounty_memory collection reference."""
    return get_collection(collection_name, db)


# Lazy re-exports for Interface Contract compliance
def claim_lead_transaction(
    db: firestore.Client,
    lead_id: str,
    worker_id: str,
    collection_name: str = "bounty_leads",
    lock_timeout_sec: int = 300
) -> bool:
    """
    Interface contract function for atomic lead claiming.
    Delegates to listener.claim_lead_atomic.
    """
    from src.core.listener import claim_lead_atomic
    return claim_lead_atomic(
        db=db,
        lead_id=lead_id,
        worker_id=worker_id,
        collection_name=collection_name,
        lock_timeout_sec=lock_timeout_sec
    )


def listen_collection(
    col_ref: Any,
    callback: Any,
    error_callback: Optional[Any] = None,
    executor: Optional[Any] = None
) -> Any:
    """
    Interface contract function for collection listening.
    Delegates to listener.listen_collection.
    """
    from src.core.listener import listen_collection as _listen_col
    return _listen_col(
        col_ref=col_ref,
        callback=callback,
        error_callback=error_callback,
        executor=executor
    )
