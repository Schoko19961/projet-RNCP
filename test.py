
from api.api import get_data
from tables.sv_cours_a import load_sv_cours_a
import dateutil.parser
# "2023-04-08T21:23:52Z"
sv_horai_a = load_sv_cours_a(step_size=24, max_days=1, start_date=dateutil.parser.isoparse("2023-08-24T00:00:00Z"))
# sv_horai_a.to_csv("sv_horai_a.csv.gzip", index=False, compression="gzip")
# sv_horai_a.to_csv("sv_horai_a.csv", index=False)
print("Test")