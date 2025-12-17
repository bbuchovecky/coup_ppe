# TODO:
# fix paths
# determine which filesystem (?)
# run coup_ppe.access.catalog.build_catalog()
# save catalog JSON file in src/coup_ppe/catalogs

"""Build intake-esm catalog for FHIST PPE ensemble."""
import intake_esm
from pathlib import Path
from coup_ppe import access

# Define paths
DATA_ROOT = Path("/glade/campaign/...")
OUTPUT_DIR = Path(__file__).parent.parent / "src/coup_ppe/catalogs"

