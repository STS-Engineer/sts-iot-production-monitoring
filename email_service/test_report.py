import traceback
from report import get_report_data

try:
    data = get_report_data()
    print("Success! Keys:", list(data.keys()))
except Exception as e:
    print("Error:", e)
    traceback.print_exc()
