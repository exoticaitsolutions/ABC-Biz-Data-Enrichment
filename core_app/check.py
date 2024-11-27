import csv
from io import TextIOWrapper

import pandas as pd
csv_file = pd.read_csv(r"C:\Users\Exotica\ABC-Biz-Data-Enrichment\core_app\Abc_biz_ Yelp restaura.csv",encoding='latin1')
reader = csv_file.to_dict(orient='index')
print(reader)

# reader = csv.DictReader(csv_file)

