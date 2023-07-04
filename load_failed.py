import json
from api.api import get_data
from tqdm import tqdm

TABLE_NAME = "sv_horai_a"

with open("error_copy.json", "r") as f:
    failed = f.read()
failed_json = json.loads(failed)
for failed in tqdm(failed_json):
    data = get_data(TABLE_NAME, failed)
    data.to_csv(f"data/sv_horai_a/sv_horai_a_{failed['backintime']}.csv", index=False)