from api.api import get_data
from tables.sv_horai_a import load_data
import dateutil.parser
# "2023-04-08T21:23:52Z"
sv_horai_a = load_data(step_size=24, max_days=365, start_date=dateutil.parser.isoparse("2023-06-26T21:23:52Z"))
# sv_horai_a.to_csv("sv_horai_a.csv.gzip", index=False, compression="gzip")
# sv_horai_a.to_csv("sv_horai_a.csv", index=False)
print("Test")