try:
    from flask import Flask, render_template, request, redirect, url_for, send_from_directory, Response
    from dotenv import load_dotenv
except ModuleNotFoundError as exc:
    missing = getattr(exc, "name", "a required package")
    raise SystemExit(
        f"Missing Python dependency: {missing}\n\n"
        "Use the virtualenv and install requirements, then run the server:\n"
        "  python3 -m venv .venv\n"
        "  source .venv/bin/activate\n"
        "  python -m pip install -r requirements.txt\n"
        "  python server.py\n\n"
        "Or run directly with:\n"
        "  .venv/bin/python server.py\n"
    ) from exc
from datetime import datetime, timedelta
import os
from pathlib import Path
import uuid
import json
import sys
import csv
import io

from werkzeug.utils import secure_filename

from storage_local import LocalStore

load_dotenv()

BASE_PATH = Path(getattr(sys, "_MEIPASS", Path(__file__).parent)).resolve()
app = Flask(
    __name__,
    template_folder=str(BASE_PATH / 'templates'),
    static_folder=str(BASE_PATH / 'static'),
)

STORAGE_BACKEND = os.environ.get('STORAGE_BACKEND', 'local').lower()
IS_VERCEL = os.environ.get('VERCEL') == '1'
DEFAULT_APP_DATA_DIR = '/tmp/lion_car_sale' if IS_VERCEL else str(Path.home() / '.lion_car_sale')
APP_DATA_DIR = os.environ.get('APP_DATA_DIR', DEFAULT_APP_DATA_DIR)
APP_DATA_PATH = Path(APP_DATA_DIR).expanduser().resolve()

try:
    APP_DATA_PATH.mkdir(parents=True, exist_ok=True)
except OSError:
    # Serverless runtimes (for example Vercel) only allow writes under /tmp.
    APP_DATA_PATH = Path('/tmp/lion_car_sale').resolve()
    APP_DATA_PATH.mkdir(parents=True, exist_ok=True)

DEFAULT_UPLOAD_FOLDER = str(APP_DATA_PATH / 'uploads')
UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', DEFAULT_UPLOAD_FOLDER)
MAX_CONTENT_LENGTH_MB = float(os.environ.get('MAX_CONTENT_LENGTH_MB', '25'))
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = int(MAX_CONTENT_LENGTH_MB * 1024 * 1024)

UPLOAD_PATH = Path(app.config['UPLOAD_FOLDER']).expanduser().resolve()
UPLOAD_PATH.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {
    'pdf', 'png', 'jpg', 'jpeg', 'webp',
    'heic', 'heif',
}

def _ensure_mongo():
    try:
        from pymongo import MongoClient
        from bson.objectid import ObjectId
    except ModuleNotFoundError as exc:
        missing = getattr(exc, "name", "a required package")
        raise RuntimeError(f"Missing dependency for MongoDB backend: {missing}") from exc

    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise RuntimeError('DATABASE_URL environment variable is required for STORAGE_BACKEND=mongo')

    client = MongoClient(database_url)
    db = client.get_default_database()
    return client, db, ObjectId

if STORAGE_BACKEND == 'mongo':
    _mongo_client, _mongo_db, _ObjectId = _ensure_mongo()
    sales_col = _mongo_db.sales
    vehicles_col = _mongo_db.vehicles
    _local_store = None
else:
    sales_col = None
    vehicles_col = None
    _local_store = LocalStore(APP_DATA_PATH / 'data.json')

def compute_sale_taxes(sale_price: float):
    taxable_value = round(float(sale_price or 0) / 1.18, 2)
    vat_amount = round(float(sale_price or 0) - taxable_value, 2)
    sscl_amount = round(taxable_value * 0.0125, 2)
    total_tax = round(vat_amount + sscl_amount, 2)
    return {
        'taxable_value': taxable_value,
        'vat_amount': vat_amount,
        'sscl_amount': sscl_amount,
        'total_tax': total_tax,
    }

def sum_expenses(expenses):
    return round(sum((e.get('amount', 0) for e in expenses)), 2)

def vehicle_base_value(vehicle: dict) -> float:
    # New records use LC value. Older records may still have CIF price.
    return float(vehicle.get('lc_value') or vehicle.get('cif_price') or 0)

def vehicle_current_value(vehicle: dict) -> float:
    base_value = vehicle_base_value(vehicle)
    expenses_total = sum_expenses(vehicle.get('expenses', []))
    return round(base_value + expenses_total, 2)

def vehicle_category(vehicle: dict) -> str:
    if bool(vehicle.get('sold')):
        return 'sold'
    category = (vehicle.get('category') or '').strip().lower()
    if category in {'shipping', 'inventory'}:
        return category
    return 'inventory'

def vehicle_category_label(vehicle: dict) -> str:
    category = vehicle_category(vehicle)
    if category == 'shipping':
        return 'Shipping'
    if category == 'inventory':
        return 'Inventory'
    return 'Sold'

def _make_suggestions() -> list[str]:
    if STORAGE_BACKEND == 'mongo':
        vehicles = list(vehicles_col.find({}, {'make': 1}))
    else:
        vehicles = _local_store.list_vehicles()
    seen = set()
    suggestions = []
    for v in vehicles:
        make = (v.get('make') or '').strip()
        if not make:
            continue
        key = make.casefold()
        if key in seen:
            continue
        seen.add(key)
        suggestions.append(make)
    suggestions.sort(key=lambda name: name.casefold())
    return suggestions

def tax_paid_total(sale: dict) -> float:
    payments = sale.get('tax_payments') or []
    if payments:
        return round(sum((float(p.get('amount') or 0) for p in payments)), 2)
    if sale.get('tax_paid'):
        return float(sale.get('tax_amount') or 0)
    return 0.0

def tax_outstanding(sale: dict) -> float:
    tax_amount = float(sale.get('tax_amount') or 0)
    return round(max(tax_amount - tax_paid_total(sale), 0.0), 2)

def tax_due_at(sale: dict):
    created_at = sale.get('created_at')
    if not created_at:
        return None
    return created_at + timedelta(days=30)

def tax_payment_enabled(sale: dict) -> bool:
    due_at = tax_due_at(sale)
    if not due_at:
        return False
    return datetime.utcnow() >= due_at

def tax_summary(sales: list[dict]):
    now = datetime.utcnow()
    liability = 0.0
    paid = 0.0
    outstanding = 0.0
    due_now = 0.0
    for s in sales:
        liability += float(s.get('tax_amount') or 0)
        s_paid = float(tax_paid_total(s) or 0)
        s_out = float(tax_outstanding(s) or 0)
        paid += s_paid
        outstanding += s_out
        due_at = tax_due_at(s)
        if due_at and due_at <= now:
            due_now += s_out
    liability = round(liability, 2)
    paid = round(paid, 2)
    outstanding = round(outstanding, 2)
    due_now = round(due_now, 2)
    progress_pct = 0 if liability <= 0 else int(round((paid / liability) * 100))
    progress_pct = max(0, min(progress_pct, 100))
    return {
        'liability': liability,
        'paid': paid,
        'outstanding': outstanding,
        'due_now': due_now,
        'progress_pct': progress_pct,
    }

def _allowed_file(filename: str) -> bool:
    if not filename or '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower()
    return ext in ALLOWED_EXTENSIONS

def _save_upload(file_storage, *, prefix: str) -> dict:
    original_name = file_storage.filename or ''
    if not _allowed_file(original_name):
        raise ValueError('Unsupported file type')

    safe_name = secure_filename(original_name)
    stored_name = f"{prefix}_{uuid.uuid4().hex}_{safe_name}"
    dest = UPLOAD_PATH / stored_name
    file_storage.save(dest)
    return {
        'original_name': original_name,
        'stored_name': stored_name,
        'content_type': file_storage.mimetype,
        'uploaded_at': datetime.utcnow(),
    }

def _month_bounds_utc(year: int, month: int):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start, end

def _sale_payment_amount_in_range(sale: dict, start: datetime, end: datetime) -> float:
    payments = sale.get('tax_payments') or []
    total = 0.0
    for p in payments:
        paid_at = p.get('paid_at')
        if not paid_at or paid_at < start or paid_at >= end:
            continue
        total += float(p.get('amount') or 0)
    return round(total, 2)

def _sale_has_payment_in_range(sale: dict, start: datetime, end: datetime) -> bool:
    payments = sale.get('tax_payments') or []
    for p in payments:
        paid_at = p.get('paid_at')
        if paid_at and start <= paid_at < end:
            return True
    return False

def _float_or_zero(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0

def ensure_sale_tax_fields(sale: dict) -> dict:
    sale_price = _float_or_zero(sale.get('sale_price'))
    if sale_price <= 0:
        sale.setdefault('taxable_value', 0.0)
        sale.setdefault('vat_amount', 0.0)
        sale.setdefault('sscl_amount', 0.0)
        sale.setdefault('tax_amount', _float_or_zero(sale.get('tax_amount')))
        return sale

    tax_values = compute_sale_taxes(sale_price)
    sale.setdefault('taxable_value', tax_values['taxable_value'])
    sale.setdefault('vat_amount', tax_values['vat_amount'])
    sale.setdefault('sscl_amount', tax_values['sscl_amount'])
    sale.setdefault('tax_amount', tax_values['total_tax'])
    return sale

def _vehicle_for_sale(sale: dict):
    vehicle_id = sale.get('vehicle_id')
    if not vehicle_id:
        return None
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
        if v:
            v['id'] = str(v.get('_id'))
        return v
    return _local_store.get_vehicle(vehicle_id)

@app.route('/uploads/<path:filename>')
def uploads(filename):
    return send_from_directory(str(UPLOAD_PATH), filename, as_attachment=False)

@app.route('/')
def index():
    # dashboard showing vehicles and recent sales
    if STORAGE_BACKEND == 'mongo':
        vehicles = list(vehicles_col.find().sort('created_at', -1))
        sales = list(sales_col.find().sort('created_at', -1).limit(10))
        for v in vehicles:
            v['id'] = str(v.get('_id'))
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
        for s in sales:
            s['id'] = str(s.get('_id'))
    else:
        vehicles = _local_store.list_vehicles()
        sales = _local_store.list_sales(limit=10)
        for v in vehicles:
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)

    if STORAGE_BACKEND == 'mongo':
        all_sales = list(
            sales_col.find(
                {},
                {
                    'tax_amount': 1,
                    'tax_payments': 1,
                    'tax_paid': 1,
                    'tax_paid_at': 1,
                    'created_at': 1,
                },
            ).sort('created_at', -1)
        )
    else:
        all_sales = _local_store.list_sales()
    for s in all_sales:
        if STORAGE_BACKEND == 'mongo':
            s['id'] = str(s.get('_id'))
        ensure_sale_tax_fields(s)
    summary = tax_summary(all_sales)
    return render_template('index.html', vehicles=vehicles, sales=sales, tax_summary=summary)

@app.route('/vehicles')
def vehicles():
    if STORAGE_BACKEND == 'mongo':
        vehicles = list(vehicles_col.find({'sold': {'$ne': True}}).sort('created_at', -1))
        for v in vehicles:
            v['id'] = str(v.get('_id'))
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
        vehicles = [v for v in vehicles if v['category'] == 'inventory']
    else:
        vehicles = _local_store.list_vehicles(sold=False)
        for v in vehicles:
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
        vehicles = [v for v in vehicles if v['category'] == 'inventory']
    return render_template('vehicles.html', vehicles=vehicles)

@app.route('/vehicles/shipping')
def shipping_vehicles():
    if STORAGE_BACKEND == 'mongo':
        vehicles = list(vehicles_col.find({'sold': {'$ne': True}}).sort('created_at', -1))
        for v in vehicles:
            v['id'] = str(v.get('_id'))
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
        vehicles = [v for v in vehicles if v['category'] == 'shipping']
    else:
        vehicles = _local_store.list_vehicles(sold=False)
        for v in vehicles:
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
        vehicles = [v for v in vehicles if v['category'] == 'shipping']
    return render_template('vehicles.html', vehicles=vehicles, view_mode='shipping')

@app.route('/vehicles/sold')
def sold_vehicles():
    if STORAGE_BACKEND == 'mongo':
        vehicles = list(vehicles_col.find({'sold': True}).sort('sold_at', -1))
        for v in vehicles:
            v['id'] = str(v.get('_id'))
            if v.get('sale_id') is not None:
                v['sale_id'] = str(v['sale_id'])
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
    else:
        vehicles = _local_store.list_sold_vehicles()
        for v in vehicles:
            v['expenses_total'] = sum_expenses(v.get('expenses', []))
            v['current_value'] = vehicle_current_value(v)
            v['category'] = vehicle_category(v)
            v['category_label'] = vehicle_category_label(v)
    return render_template('sold_vehicles.html', vehicles=vehicles)

@app.route('/vehicles/create', methods=['GET', 'POST'])
def create_vehicle():
    if request.method == 'POST':
        form = request.form
        lc_value = float(form.get('lc_value') or 0)
        doc = {
            'file_number': form.get('file_number','').strip(),
            'make': form.get('make',''),
            'model': form.get('model',''),
            'model_code': form.get('model_code',''),
            'chassis_no': form.get('chassis_no',''),
            'lc_value': lc_value,
            'lc_no': form.get('lc_no',''),
            'year': form.get('year',''),
            'color': form.get('color',''),
            'expenses': [],
            'category': 'shipping',
            'vat_paid_on_inventory': 0.0,
            'vat_paid_on_inventory_at': None,
            'sold': False,
            'created_at': datetime.utcnow(),
        }
        if STORAGE_BACKEND == 'mongo':
            res = vehicles_col.insert_one(doc)
            return redirect(url_for('vehicle_detail', vehicle_id=str(res.inserted_id)))
        res = _local_store.create_vehicle(doc)
        return redirect(url_for('vehicle_detail', vehicle_id=res.inserted_id))
    return render_template('vehicle_form.html', vehicle=None, make_suggestions=_make_suggestions())

@app.route('/vehicles/<vehicle_id>')
def vehicle_detail(vehicle_id):
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
        if v:
            v['id'] = str(v.get('_id'))
            if v.get('sale_id') is not None:
                v['sale_id'] = str(v['sale_id'])
    else:
        v = _local_store.get_vehicle(vehicle_id)
    if not v:
        return 'Vehicle not found', 404
    v['category'] = vehicle_category(v)
    v['category_label'] = vehicle_category_label(v)
    total_expenses = sum_expenses(v.get('expenses', []))
    current_value = vehicle_current_value(v)
    return render_template('vehicle_detail.html', vehicle=v, total_expenses=total_expenses, current_value=current_value)

@app.route('/vehicles/<vehicle_id>/move-to-inventory', methods=['POST'])
def move_vehicle_to_inventory(vehicle_id):
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
    else:
        v = _local_store.get_vehicle(vehicle_id)
    if not v:
        return 'Vehicle not found', 404
    if bool(v.get('sold')):
        return redirect(url_for('vehicle_detail', vehicle_id=vehicle_id))

    vat_paid_amount = _float_or_zero(request.form.get('vat_paid_amount'))
    if vat_paid_amount < 0:
        return 'VAT paid amount cannot be negative', 400

    paid_at = datetime.utcnow()

    if STORAGE_BACKEND == 'mongo':
        vehicles_col.update_one(
            {'_id': _ObjectId(vehicle_id)},
            {
                '$set': {
                    'category': 'inventory',
                    'vat_paid_on_inventory': round(vat_paid_amount, 2),
                    'vat_paid_on_inventory_at': paid_at,
                }
            },
        )
    else:
        _local_store.move_vehicle_to_inventory(vehicle_id, vat_paid_amount=vat_paid_amount, paid_at=paid_at)
    return redirect(url_for('vehicle_detail', vehicle_id=vehicle_id))

@app.route('/vehicles/<vehicle_id>/expense', methods=['POST'])
def add_expense(vehicle_id):
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
    else:
        v = _local_store.get_vehicle(vehicle_id)
    if not v:
        return 'Vehicle not found', 404
    form = request.form
    receipt_meta = None
    receipt_file = request.files.get('receipt')
    if receipt_file and receipt_file.filename:
        try:
            receipt_meta = _save_upload(receipt_file, prefix=f"expense_{vehicle_id}")
        except ValueError as exc:
            return str(exc), 400

    expense = {
        'date': form.get('date') or datetime.utcnow().isoformat(),
        'description1': form.get('description1',''),
        'description2': form.get('description2',''),
        'category': form.get('category','Others'),
        'amount': float(form.get('amount') or 0),
    }
    if receipt_meta:
        expense['receipt'] = receipt_meta

    if STORAGE_BACKEND == 'mongo':
        vehicles_col.update_one({'_id': _ObjectId(vehicle_id)}, {'$push': {'expenses': expense}})
    else:
        _local_store.push_vehicle_expense(vehicle_id, expense)
    return redirect(url_for('vehicle_detail', vehicle_id=vehicle_id))

@app.route('/vehicles/<vehicle_id>/documents', methods=['POST'])
def add_vehicle_document(vehicle_id):
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
    else:
        v = _local_store.get_vehicle(vehicle_id)
    if not v:
        return 'Vehicle not found', 404

    doc_file = request.files.get('document')
    if not doc_file or not doc_file.filename:
        return 'Document file is required', 400

    try:
        meta = _save_upload(doc_file, prefix=f"vehicle_{vehicle_id}")
    except ValueError as exc:
        return str(exc), 400

    meta['label'] = (request.form.get('label') or '').strip()
    if STORAGE_BACKEND == 'mongo':
        vehicles_col.update_one({'_id': _ObjectId(vehicle_id)}, {'$push': {'documents': meta}})
    else:
        _local_store.push_vehicle_document(vehicle_id, meta)
    return redirect(url_for('vehicle_detail', vehicle_id=vehicle_id))

@app.route('/vehicles/<vehicle_id>/sell', methods=['POST'])
def sell_vehicle(vehicle_id):
    if STORAGE_BACKEND == 'mongo':
        v = vehicles_col.find_one({'_id': _ObjectId(vehicle_id)})
    else:
        v = _local_store.get_vehicle(vehicle_id)
    if not v:
        return 'Vehicle not found', 404
    if vehicle_category(v) != 'inventory':
        return 'Vehicle must be moved to inventory before marking as sold', 400
    form = request.form
    sale_price = float(form.get('sale_price') or 0)
    buyer_name = (form.get('buyer_name') or '').strip()
    buyer_address = (form.get('buyer_address') or '').strip()
    buyer_phone = (form.get('buyer_phone') or '').strip()
    payment_method = form.get('payment_method','Cash')
    notes = form.get('notes','')
    tax_values = compute_sale_taxes(sale_price)
    taxable_value = tax_values['taxable_value']
    vat_amount = tax_values['vat_amount']
    sscl_amount = tax_values['sscl_amount']
    tax_amount = tax_values['total_tax']

    vat_paid_on_inventory = round(min(_float_or_zero(v.get('vat_paid_on_inventory')), vat_amount), 2)
    vat_paid_on_inventory_at = v.get('vat_paid_on_inventory_at') or datetime.utcnow()
    tax_payments = []
    if vat_paid_on_inventory > 0:
        tax_payments.append(
            {
                'amount': vat_paid_on_inventory,
                'paid_at': vat_paid_on_inventory_at,
                'note': 'VAT paid when vehicle moved to inventory',
                'source': 'inventory_vat',
            }
        )

    total_expenses = sum_expenses(v.get('expenses', []))
    cost_basis = vehicle_base_value(v) + total_expenses
    net_profit = round(sale_price - cost_basis - tax_amount, 2)

    sale_doc = {
        'vehicle_id': vehicle_id,
        'buyer_name': buyer_name,
        'buyer_address': buyer_address,
        'buyer_phone': buyer_phone,
        'sale_price': sale_price,
        'taxable_value': taxable_value,
        'vat_amount': vat_amount,
        'sscl_amount': sscl_amount,
        'tax_amount': tax_amount,
        'tax_payments': tax_payments,
        'cost_basis': cost_basis,
        'total_expenses': total_expenses,
        'net_profit': net_profit,
        'payment_method': payment_method,
        'notes': notes,
        'created_at': datetime.utcnow(),
    }
    if STORAGE_BACKEND == 'mongo':
        res = sales_col.insert_one(sale_doc)
        vehicles_col.update_one(
            {'_id': _ObjectId(vehicle_id)},
            {'$set': {'sold': True, 'category': 'sold', 'sold_at': datetime.utcnow(), 'sale_id': res.inserted_id}},
        )
        return redirect(url_for('receipt', sale_id=str(res.inserted_id)))
    res = _local_store.create_sale(sale_doc)
    _local_store.set_vehicle_sold(vehicle_id, sale_id=res.inserted_id)
    return redirect(url_for('receipt', sale_id=res.inserted_id))

@app.route('/receipt/<sale_id>')
def receipt(sale_id):
    if STORAGE_BACKEND == 'mongo':
        sale = sales_col.find_one({'_id': _ObjectId(sale_id)})
        if sale:
            sale['id'] = str(sale.get('_id'))
    else:
        sale = _local_store.get_sale(sale_id)
    if not sale:
        return 'Sale not found', 404
    ensure_sale_tax_fields(sale)
    if STORAGE_BACKEND == 'mongo':
        vehicle = vehicles_col.find_one({'_id': _ObjectId(sale.get('vehicle_id'))}) if sale.get('vehicle_id') else None
        if vehicle:
            vehicle['id'] = str(vehicle.get('_id'))
    else:
        vehicle = _local_store.get_vehicle(sale.get('vehicle_id')) if sale.get('vehicle_id') else None
    return render_template(
        'receipt.html',
        sale=sale,
        vehicle=vehicle,
        tax_paid_total=tax_paid_total(sale),
        tax_outstanding=tax_outstanding(sale),
        tax_due_at=tax_due_at(sale),
        tax_payment_enabled=tax_payment_enabled(sale),
    )

@app.route('/sales')
def sales():
    if STORAGE_BACKEND == 'mongo':
        sales = list(sales_col.find().sort('created_at', -1))
        for s in sales:
            s['id'] = str(s.get('_id'))
        vehicle_ids = [s.get('vehicle_id') for s in sales if s.get('vehicle_id')]
        vehicle_map = {}
        if vehicle_ids:
            for vehicle in vehicles_col.find({'_id': {'$in': [_ObjectId(v_id) for v_id in vehicle_ids]}}):
                vehicle_map[str(vehicle.get('_id'))] = vehicle
        for s in sales:
            s['vehicle'] = vehicle_map.get(s.get('vehicle_id'))
    else:
        sales = _local_store.list_sales()
        vehicle_map = {}
        for s in sales:
            vehicle_id = s.get('vehicle_id')
            if not vehicle_id:
                continue
            if vehicle_id not in vehicle_map:
                vehicle_map[vehicle_id] = _local_store.get_vehicle(vehicle_id)
            s['vehicle'] = vehicle_map.get(vehicle_id)
    for s in sales:
        ensure_sale_tax_fields(s)
        s['tax_paid_total'] = tax_paid_total(s)
        s['tax_outstanding'] = tax_outstanding(s)
        s['tax_due_at'] = tax_due_at(s)
    return render_template('sales.html', sales=sales)

@app.route('/sales/<sale_id>/tax-payment', methods=['POST'])
def add_tax_payment(sale_id):
    if STORAGE_BACKEND == 'mongo':
        sale = sales_col.find_one({'_id': _ObjectId(sale_id)})
    else:
        sale = _local_store.get_sale(sale_id)
    if not sale:
        return 'Sale not found', 404

    if not tax_payment_enabled(sale):
        due_at = tax_due_at(sale)
        msg = f"Tax payments are enabled after {due_at.strftime('%Y-%m-%d')}" if due_at else "Tax payments are not enabled yet"
        return msg, 400

    try:
        amount = float(request.form.get('amount') or 0)
    except ValueError:
        return 'Invalid amount', 400
    if amount <= 0:
        return 'Amount must be greater than 0', 400

    outstanding = tax_outstanding(sale)
    if amount - outstanding > 0.0001:
        return f'Amount exceeds outstanding tax (${outstanding:.2f})', 400

    note = (request.form.get('note') or '').strip()
    paid_at = datetime.utcnow()
    receipt_meta = None
    receipt_file = request.files.get('receipt')
    if receipt_file and receipt_file.filename:
        try:
            receipt_meta = _save_upload(receipt_file, prefix=f"tax_{sale_id}")
        except ValueError as exc:
            return str(exc), 400
    tax_payment = {
        'amount': round(amount, 2),
        'paid_at': paid_at,
        'note': note,
    }
    if receipt_meta:
        tax_payment['receipt'] = receipt_meta

    if STORAGE_BACKEND == 'mongo':
        sales_col.update_one({'_id': _ObjectId(sale_id)}, {'$push': {'tax_payments': tax_payment}})
    else:
        _local_store.push_tax_payment(sale_id, tax_payment)
    next_url = request.form.get('next') or url_for('receipt', sale_id=sale_id)
    return redirect(next_url)

@app.route('/tax')
def tax_manage():
    scope = (request.args.get('scope') or 'all').lower()
    now_year = datetime.utcnow().year
    now_month = datetime.utcnow().month
    try:
        year = int(request.args.get('year') or now_year)
    except ValueError:
        year = now_year

    if scope == 'year':
        start, end = _year_bounds_utc(year)
        if STORAGE_BACKEND == 'mongo':
            sales = list(sales_col.find({'created_at': {'$gte': start, '$lt': end}}).sort('created_at', -1))
            for s in sales:
                s['id'] = str(s.get('_id'))
        else:
            sales = _local_store.sales_between(start, end)
    else:
        if STORAGE_BACKEND == 'mongo':
            sales = list(sales_col.find().sort('created_at', -1))
            for s in sales:
                s['id'] = str(s.get('_id'))
        else:
            sales = _local_store.list_sales()

    for s in sales:
        ensure_sale_tax_fields(s)
        s['tax_paid_total'] = tax_paid_total(s)
        s['tax_outstanding'] = tax_outstanding(s)
        s['tax_due_at'] = tax_due_at(s)
        s['tax_payment_enabled'] = tax_payment_enabled(s)

    summary = tax_summary(sales)
    return render_template('tax_manage.html', sales=sales, summary=summary, scope=scope, year=year, now_month=now_month)

@app.route('/tax/monthly')
def tax_monthly_statement():
    now = datetime.utcnow()
    try:
        year = int(request.args.get('year') or now.year)
        month = int(request.args.get('month') or now.month)
    except ValueError:
        return 'Invalid year/month', 400
    if month < 1 or month > 12:
        return 'Invalid month', 400

    start, end = _month_bounds_utc(year, month)

    # Sales whose tax due date falls in the selected month:
    created_start = start - timedelta(days=30)
    created_end = end - timedelta(days=30)

    if STORAGE_BACKEND == 'mongo':
        candidates = list(sales_col.find({'created_at': {'$gte': created_start, '$lt': created_end}}))
        for s in candidates:
            s['id'] = str(s.get('_id'))
    else:
        candidates = _local_store.sales_between(created_start, created_end)

    sales_due = []
    for s in candidates:
        ensure_sale_tax_fields(s)
        due_at = tax_due_at(s)
        if not due_at or due_at < start or due_at >= end:
            continue
        s['tax_due_at'] = due_at
        s['tax_paid_total'] = tax_paid_total(s)
        s['tax_outstanding'] = tax_outstanding(s)
        s['tax_paid_in_month'] = _sale_payment_amount_in_range(s, start, end)
        v = _vehicle_for_sale(s)
        if v:
            s['vehicle'] = v
        sales_due.append(s)

    # Also include sales that had payments in the month (even if not due that month)
    if STORAGE_BACKEND == 'mongo':
        paid_candidates = list(sales_col.find({'tax_payments.paid_at': {'$gte': start, '$lt': end}}))
        for s in paid_candidates:
            s['id'] = str(s.get('_id'))
    else:
        paid_candidates = _local_store.sales_with_payments_between(start, end)

    paid_map = {s['id']: s for s in sales_due}
    for s in paid_candidates:
        ensure_sale_tax_fields(s)
        sid = s.get('id') or str(s.get('_id')) if STORAGE_BACKEND == 'mongo' else s.get('id')
        if sid in paid_map:
            continue
        if not _sale_has_payment_in_range(s, start, end):
            continue
        s['tax_due_at'] = tax_due_at(s)
        s['tax_paid_total'] = tax_paid_total(s)
        s['tax_outstanding'] = tax_outstanding(s)
        s['tax_paid_in_month'] = _sale_payment_amount_in_range(s, start, end)
        v = _vehicle_for_sale(s)
        if v:
            s['vehicle'] = v
        sales_due.append(s)

    sales_due.sort(key=lambda s: s.get('tax_due_at') or datetime.min)

    totals = {
        'tax_liability': round(sum(float(s.get('tax_amount') or 0) for s in sales_due), 2),
        'tax_paid_in_month': round(sum(float(s.get('tax_paid_in_month') or 0) for s in sales_due), 2),
        'tax_paid_total': round(sum(float(s.get('tax_paid_total') or 0) for s in sales_due), 2),
        'tax_outstanding': round(sum(float(s.get('tax_outstanding') or 0) for s in sales_due), 2),
    }

    return render_template('tax_monthly_statement.html', year=year, month=month, start=start, end=end, sales=sales_due, totals=totals)

@app.route('/tax/monthly.csv')
def tax_monthly_statement_csv():
    now = datetime.utcnow()
    try:
        year = int(request.args.get('year') or now.year)
        month = int(request.args.get('month') or now.month)
    except ValueError:
        return 'Invalid year/month', 400
    if month < 1 or month > 12:
        return 'Invalid month', 400

    start, end = _month_bounds_utc(year, month)
    created_start = start - timedelta(days=30)
    created_end = end - timedelta(days=30)

    if STORAGE_BACKEND == 'mongo':
        candidates = list(sales_col.find({'created_at': {'$gte': created_start, '$lt': created_end}}))
        for s in candidates:
            s['id'] = str(s.get('_id'))
        paid_candidates = list(sales_col.find({'tax_payments.paid_at': {'$gte': start, '$lt': end}}))
        for s in paid_candidates:
            s['id'] = str(s.get('_id'))
    else:
        candidates = _local_store.sales_between(created_start, created_end)
        paid_candidates = _local_store.sales_with_payments_between(start, end)

    sales = []
    seen = set()
    for s in candidates + paid_candidates:
        ensure_sale_tax_fields(s)
        sid = s.get('id') or (str(s.get('_id')) if STORAGE_BACKEND == 'mongo' else None)
        if not sid or sid in seen:
            continue
        seen.add(sid)

        due_at = tax_due_at(s)
        if due_at and (start <= due_at < end) or _sale_has_payment_in_range(s, start, end):
            v = _vehicle_for_sale(s) or {}
            row = {
                'sale_id': sid,
                'sale_date': (s.get('created_at').strftime('%Y-%m-%d') if s.get('created_at') else ''),
                'due_date': (due_at.strftime('%Y-%m-%d') if due_at else ''),
                'chassis_no': v.get('chassis_no', ''),
                'make': v.get('make', ''),
                'model': v.get('model', ''),
                'taxable_value': f"{float(s.get('taxable_value') or 0):.2f}",
                'vat_amount': f"{float(s.get('vat_amount') or 0):.2f}",
                'sscl_amount': f"{float(s.get('sscl_amount') or 0):.2f}",
                'tax_amount': f"{float(s.get('tax_amount') or 0):.2f}",
                'tax_paid_in_month': f"{_sale_payment_amount_in_range(s, start, end):.2f}",
                'tax_paid_total': f"{tax_paid_total(s):.2f}",
                'tax_outstanding': f"{tax_outstanding(s):.2f}",
            }
            sales.append(row)

    sales.sort(key=lambda r: (r.get('due_date') or '9999-12-31', r.get('sale_date') or ''))

    buf = io.StringIO()
    fieldnames = ['sale_id', 'sale_date', 'due_date', 'chassis_no', 'make', 'model', 'taxable_value', 'vat_amount', 'sscl_amount', 'tax_amount', 'tax_paid_in_month', 'tax_paid_total', 'tax_outstanding']
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in sales:
        writer.writerow(r)

    filename = f"tax_statement_{year:04d}_{month:02d}.csv"
    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'},
    )

def _year_bounds_utc(year: int):
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    return start, end

@app.route('/reports/taxes')
def tax_report():
    now_year = datetime.utcnow().year
    try:
        year = int(request.args.get('year') or now_year)
    except ValueError:
        year = now_year

    start, end = _year_bounds_utc(year)
    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_sales = [0.0] * 12
    monthly_liability = [0.0] * 12
    monthly_paid = [0.0] * 12
    monthly_due = [0.0] * 12

    now = datetime.utcnow()
    earliest_for_due = start - timedelta(days=31)

    if STORAGE_BACKEND == 'mongo':
        sales_for_year = list(sales_col.find({'created_at': {'$gte': start, '$lt': end}}))
    else:
        sales_for_year = _local_store.sales_between(start, end)
    for s in sales_for_year:
        ensure_sale_tax_fields(s)
        created_at = s.get('created_at')
        if not created_at:
            continue
        idx = created_at.month - 1
        monthly_sales[idx] += float(s.get('sale_price') or 0)
        monthly_liability[idx] += float(s.get('tax_amount') or 0)

    if STORAGE_BACKEND == 'mongo':
        sales_for_due = list(sales_col.find({'created_at': {'$gte': earliest_for_due, '$lt': end}}))
    else:
        sales_for_due = _local_store.sales_between(earliest_for_due, end)
    for s in sales_for_due:
        ensure_sale_tax_fields(s)
        due_at = tax_due_at(s)
        if not due_at or due_at < start or due_at >= end or due_at > now:
            continue
        idx = due_at.month - 1
        monthly_due[idx] += float(tax_outstanding(s) or 0)

    if STORAGE_BACKEND == 'mongo':
        sales_with_payments = list(
            sales_col.find(
                {
                    '$or': [
                        {'tax_payments.paid_at': {'$gte': start, '$lt': end}},
                        {'tax_paid_at': {'$gte': start, '$lt': end}},
                    ]
                }
            )
        )
    else:
        sales_with_payments = _local_store.sales_with_payments_between(start, end)
    for s in sales_with_payments:
        ensure_sale_tax_fields(s)
        payments = s.get('tax_payments') or []
        for p in payments:
            paid_at = p.get('paid_at')
            if not paid_at or paid_at < start or paid_at >= end:
                continue
            idx = paid_at.month - 1
            monthly_paid[idx] += float(p.get('amount') or 0)
        if s.get('tax_paid') and not payments:
            paid_at = s.get('tax_paid_at') or s.get('created_at')
            if paid_at and start <= paid_at < end:
                idx = paid_at.month - 1
                monthly_paid[idx] += float(s.get('tax_amount') or 0)

    monthly_sales = [round(v, 2) for v in monthly_sales]
    monthly_liability = [round(v, 2) for v in monthly_liability]
    monthly_paid = [round(v, 2) for v in monthly_paid]
    monthly_due = [round(v, 2) for v in monthly_due]

    totals = {
        'sales_total': round(sum(monthly_sales), 2),
        'tax_liability': round(sum(monthly_liability), 2),
        'tax_paid': round(sum(monthly_paid), 2),
        'tax_due': round(sum(monthly_due), 2),
    }

    chart_data = {
        'labels': labels,
        'monthly_sales': monthly_sales,
        'monthly_tax_liability': monthly_liability,
        'monthly_tax_paid': monthly_paid,
        'monthly_tax_due': monthly_due,
    }

    return render_template(
        'tax_report.html',
        year=year,
        totals=totals,
        chart_data=json.dumps(chart_data),
    )

if __name__ == '__main__':
    app.run(debug=True)
