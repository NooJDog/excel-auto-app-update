# modules/report_generator.py
import os
import pandas as pd
from datetime import datetime

class ReportGenerator:
    def __init__(self, db_manager, output_folder):
        self.db = db_manager
        self.output = os.path.abspath(output_folder)
        os.makedirs(self.output, exist_ok=True)

    def generate_cumulative_report(self):
        rows = self.db.query_all()
        if not rows:
            return None
        df = pd.DataFrame(rows)
        # simple cumulative balance if missing
        if "balance" not in df.columns or df["balance"].isnull().all():
            df = df.sort_values(["date", "id"])
            df["balance_calc"] = (df["income"].fillna(0) - df["expense"].fillna(0)).cumsum()
            df["balance"] = df["balance_calc"]
            df.drop(columns=["balance_calc"], inplace=True)
        out_path = os.path.join(self.output, "cumulative_report.xlsx")
        df.to_excel(out_path, index=False)
        return out_path

    def generate_daily_report(self):
        rows = self.db.query_all()
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df_group = df.groupby("date", as_index=False).agg({
            "income": "sum",
            "expense": "sum",
            "id": "count"
        }).rename(columns={"id": "transactions"})
        out_path = os.path.join(self.output, f"daily_report_{datetime.now().strftime('%Y%m%d')}.xlsx")
        df_group.to_excel(out_path, index=False)
        return out_path