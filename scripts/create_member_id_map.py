# TODO:
# fix paths

"""Create JSON dictionary maps for member IDs."""
from pathlib import Path
import coup_ppe.metadata.members

# Define paths
DATA_ROOT = Path("/glade/campaign/...")
OUTPUT_DIR = Path(__file__).parent.parent / "src/coup_ppe/catalogs"

coup_ppe.metadata.members.create_maps_yaml_from_crosswalk_csv(
    csv_rootpath="../crosswalk/",
    yaml_outpath="../src/coup_ppe/metadata/"
)
