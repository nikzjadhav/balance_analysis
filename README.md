# Balance Sync Log Analyzer

This project provides a PySpark + Streamlit-based tool for analyzing **subscriber balance sync logs** . It helps accountants and engineers identify **overdrafts**, reconcile discrepancies, and gain insights into **payment trends** and **anomalies** across users.

---

## ✨ Features

- ✅ **Log Parsing:** Automatically reads `.gz` log files from the `data` directory.
- 🧠 **Field Extraction:** Extracts key fields from logs including `transaction_id`, `amount`, `userId`, `oldBalance`, `newBalance`, etc.
- 🔎 **Filtering:** Filter by:
  - Log level (`INFO`, `ERROR`, etc.)
  - User ID
  - Overdraft status
- 📊 **Interactive UI:** Searchable and sortable log viewer in your browser via Streamlit.
- 📥 **CSV Export:** Download filtered results for offline analysis or audits.

---

## ⚙️ Requirements
- Download the [data zip](https://prod-files-secure.s3.us-west-2.amazonaws.com/93077ce4-41cf-40f7-9fd2-9f32ce6032db/01d008ec-e21a-4377-9bfe-c86025aeab9e/balance-sync-logs.zip) or access [here] (https://calo.notion.site/Data-Engineer-Test-Task-8757782f44ad4097ba815ea06aee58cd)
- Python 3.8+
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Streamlit](https://streamlit.io/)
- pandas

---

## 🧰 Setup & Run

### 1. **Clean and Prepare Data**

Log files need to be cleaned and copied to the `data-cleaned` directory for analysis.

```bash
bash clean-data.sh
````

### 2. **Run the Application**

Use Docker Compose to build and launch the app:

```bash
docker compose up --build
```

### 3. **Access the UI**

Once the containers are up, open your browser and visit:

```
http://localhost:8501
```

---

## 🧑‍💻 Usage

* Filter logs via sidebar:

  * Select log levels
  * Filter by `userId` or overdraft condition
* Explore entries in the interactive log table
* Click **Download CSV** to export your view

---

## 📁 Project Structure

```
balance_analysis/
├── data/               # Raw .gz logs (input)
├── data-cleaned/       # Cleaned logs after running clean-data.sh
├── output/             # Parsed CSV output
├── src/
│   ├── main.py         # CLI: Parses logs and writes structured CSV
│   └── app.py          # Streamlit UI for interactive analysis
├── Dockerfile
├── docker-compose.yml
├── clean-data.sh       # Script to preprocess data folder
└── README.md
```

---

## ⚠️ Notes

* The parser assumes a standard log structure with embedded JSON-like `transaction` blocks.
* Logs missing the `transaction` block are ignored.
* An **overdraft** is flagged when `newBalance` is less than `0`.
* Ideal for reconciliation automation or anomaly detection workflows.

---

## 📌 Example Fields Extracted

* `transaction_id`
* `type` (DEBIT / CREDIT)
* `userId`
* `amount`
* `currency`
* `oldBalance`
* `newBalance`
* `overdraft_flag`
