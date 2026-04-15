# Car Sale CLI

Simple Python CLI to record car sales, edit entries, and print receipts.

Usage

1. Ensure you have Python 3 installed.
2. Run:

```bash
python3 app.py
```

Commands

- `New sale` — enter buyer and car details, price and tax rate; saves to `sales.json` and prints a receipt.
- `Edit sale` — pick a receipt by ID and edit fields (defaults are shown).
- `List sales` — show saved sales with IDs.
- `View receipt` — print a formatted receipt for a saved sale.

Files

- `app.py`: the CLI program
- `sales.json`: data file where sales are stored

If you want, I can add a simple GUI or export receipts to PDF next.

Web version (Flask)

1. Create/activate a virtualenv (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies (into the same Python you’ll run):

```bash
python -m pip install -r requirements.txt
```

If you see `ModuleNotFoundError: No module named 'flask'`, you’re likely running `server.py` outside the virtualenv—use `python server.py` after activating, or `.venv/bin/python server.py`.

3. Run the web server (defaults to local storage):

```bash
python server.py
```

4. Open http://127.0.0.1:5000 in your browser.

MongoDB (optional)

- Set `STORAGE_BACKEND=mongo` and `DATABASE_URL` (example uses your MongoDB connection string):

```bash
export STORAGE_BACKEND=mongo
export DATABASE_URL="mongodb+srv://geemal:Fernando1976@geemal.z9d7ccy.mongodb.net/lion_car_sale?retryWrites=true&w=majority&appName=geemal"
python server.py
```

Uploads

- Expense receipts: when adding an expense, you can upload a receipt scan (PDF or image).
- Vehicle documents: upload any other scans (PDF or image) from the vehicle page.
- Files are stored locally in `~/.lion_car_sale/uploads/` by default (configurable via `UPLOAD_FOLDER` in `.env`).

Reports

- Monthly tax chart: open `/reports/taxes` (tax can be recorded as partial payments after the due date, 30 days after the sale).
- Monthly tax document: open `/tax` → “Monthly doc” to view, print/save PDF, or download CSV for a month.

Desktop app (macOS)

1. Install desktop dependencies:

```bash
source .venv/bin/activate
python -m pip install -r requirements-desktop.txt
```

2. Run the desktop app:

```bash
python desktop.py
```

Build a `.app` (optional)

```bash
source .venv/bin/activate
python -m pip install -r requirements-desktop.txt
pyinstaller --windowed --name LionCarSale desktop.py --add-data "templates:templates" --add-data "static:static"
open dist/LionCarSale.app
```

Local storage

- Default storage is local: data is saved to `~/.lion_car_sale/data.json` (set `STORAGE_BACKEND=local` and `APP_DATA_DIR` in `.env`).
- To use MongoDB instead, set `STORAGE_BACKEND=mongo` and provide `DATABASE_URL`.
