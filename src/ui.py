import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

from main import run_pipeline


class FraudAnalysisUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Banking Fraud Analysis")
        self.root.geometry("640x320")

        self.transactions_path = tk.StringVar(value="")
        self.customers_path = tk.StringVar(value="")
        self.merchants_path = tk.StringVar(value="")

        tk.Label(root, text="Select CSV files for fraud analysis", font=("Segoe UI", 14, "bold")).pack(pady=(16, 12))

        self._build_file_row("Transactions CSV", self.transactions_path)
        self._build_file_row("Customers CSV", self.customers_path)
        self._build_file_row("Merchants CSV", self.merchants_path)

        btn_frame = tk.Frame(root)
        btn_frame.pack(pady=16)
        tk.Button(btn_frame, text="Run Analysis", width=16, command=self.run_analysis).pack(side=tk.LEFT, padx=8)
        tk.Button(btn_frame, text="Browse Default Folder", width=20, command=self.pick_default_folder).pack(side=tk.LEFT, padx=8)

        self.result_text = tk.Text(root, height=6, width=70, wrap=tk.WORD)
        self.result_text.pack(padx=24, pady=(0, 8))
        self.result_text.insert(tk.END, "Analysis summary will appear here.")

        self.tree = ttk.Treeview(root, columns=("transaction_id", "customer_id", "amount", "fraud_reason"), show="headings", height=8)
        self.tree.heading("transaction_id", text="Transaction ID")
        self.tree.heading("customer_id", text="Customer ID")
        self.tree.heading("amount", text="Amount")
        self.tree.heading("fraud_reason", text="Fraud Reason")
        self.tree.column("transaction_id", width=130, anchor="center")
        self.tree.column("customer_id", width=120, anchor="center")
        self.tree.column("amount", width=90, anchor="center")
        self.tree.column("fraud_reason", width=220, anchor="w")
        self.tree.pack(padx=24, pady=(0, 16), fill=tk.BOTH, expand=True)

    def _build_file_row(self, label_text: str, var: tk.StringVar) -> None:
        frame = tk.Frame(self.root)
        frame.pack(fill=tk.X, padx=24, pady=6)

        tk.Label(frame, text=label_text, width=16, anchor="w").pack(side=tk.LEFT)
        entry = tk.Entry(frame, textvariable=var, width=44)
        entry.pack(side=tk.LEFT, padx=6)
        tk.Button(frame, text="Browse", command=lambda v=var: self._browse_file(v)).pack(side=tk.LEFT)

    def _browse_file(self, var: tk.StringVar) -> None:
        file_path = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
        )
        if file_path:
            var.set(file_path)

    def pick_default_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select folder containing the CSV files")
        if folder:
            default_dir = Path(folder)
            for var in (self.transactions_path, self.customers_path, self.merchants_path):
                current = var.get()
                if not current:
                    file_name = self._guess_filename(var)
                    candidate = default_dir / file_name
                    if candidate.exists():
                        var.set(str(candidate))

    def _guess_filename(self, var: tk.StringVar) -> str:
        if var is self.transactions_path:
            return "banking_transactions_20000.csv"
        if var is self.customers_path:
            return "customers_1000.csv"
        return "merchants.csv"

    def run_analysis(self) -> None:
        transactions_path = self.transactions_path.get().strip()
        customers_path = self.customers_path.get().strip()
        merchants_path = self.merchants_path.get().strip()

        if not transactions_path or not customers_path or not merchants_path:
            messagebox.showwarning("Missing files", "Please select all three CSV files before running the analysis.")
            return

        try:
            results = run_pipeline(
                transactions_path=transactions_path,
                customers_path=customers_path,
                merchants_path=merchants_path,
                return_results=True,
            )

            summary_report = ""
            if results:
                flagged_df = results["flagged_transactions"]
                summary_report = (
                    f"Loaded and cleaned datasets:\n"
                    f"- transactions: {results['transactions'].count()}\n"
                    f"- customers: {results['customers'].count()}\n"
                    f"- merchants: {results['merchants'].count()}\n\n"
                    f"Fraud detection results:\n"
                    f"- flagged transactions: {flagged_df.count()}\n"
                    f"- distinct fraud reasons: {flagged_df.select('fraud_reason').where('fraud_reason IS NOT NULL').distinct().count()}"
                )

            self.result_text.delete("1.0", tk.END)
            self.result_text.insert(tk.END, summary_report or "No analysis output generated.")

            for row in self.tree.get_children():
                self.tree.delete(row)

            if results:
                flagged_df = results["flagged_transactions"]
                rows = flagged_df.select("transaction_id", "customer_id", "amount", "fraud_reason").collect()
                for item in rows:
                    self.tree.insert(
                        "",
                        tk.END,
                        values=(
                            item["transaction_id"],
                            item["customer_id"],
                            item["amount"],
                            item["fraud_reason"],
                        ),
                    )
        except Exception as exc:  # pragma: no cover - UI feedback path
            messagebox.showerror("Analysis failed", str(exc))


def main() -> None:
    root = tk.Tk()
    FraudAnalysisUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
