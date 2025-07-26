import pandas as pd
import json
import sys

if __name__ == "__main__":
    csv_path = sys.argv[1]
    json_path = sys.argv[2] if len(sys.argv) > 2 else "data.json"

    df = pd.read_csv(csv_path, dtype=str)

    json_data = df.to_json(orient="records", date_format="iso")

    # 将 JSON 数据写入文件
    with open(json_path, "w") as json_file:
        json.dump(json.loads(json_data), json_file, ensure_ascii=False, indent=4)