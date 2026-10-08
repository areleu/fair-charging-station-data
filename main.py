import sys

from parser.rename import get_renamed_bnetza

# The snapshot path can be passed as the first command line argument. It
# defaults to the archive snapshot, which is named ..._08_2025.xlsx but
# actually contains the July 2025 data (Stand 18.07.2025); the paper's
# German values come from the May 2025 snapshot (Stand 07.05.2025).
DEFAULT_SNAPSHOT = "sources/BNETZA/bnetza_charging_stations_raw_08_2025.xlsx"

snapshot = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SNAPSHOT

get_renamed_bnetza("data", snapshot)
