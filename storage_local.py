import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import threading
import uuid


def _utcnow():
    return datetime.utcnow()


def _dt_to_iso(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _iso_to_dt(value):
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _normalize_datetimes(obj):
    if isinstance(obj, dict):
        return {k: _normalize_datetimes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_datetimes(v) for v in obj]
    return _iso_to_dt(obj)


def _serialize(obj):
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    return _dt_to_iso(obj)


@dataclass(frozen=True)
class InsertResult:
    inserted_id: str


class LocalStore:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._ensure()

    def _ensure(self):
        if self.path.exists():
            return
        self._write({'vehicles': [], 'sales': []})

    def _read(self):
        raw = self.path.read_text('utf-8')
        data = json.loads(raw) if raw.strip() else {'vehicles': [], 'sales': []}
        data.setdefault('vehicles', [])
        data.setdefault('sales', [])
        return _normalize_datetimes(data)

    def _write(self, data):
        tmp = self.path.with_suffix(self.path.suffix + '.tmp')
        tmp.write_text(json.dumps(_serialize(data), indent=2, ensure_ascii=False), 'utf-8')
        tmp.replace(self.path)

    # Vehicles
    def list_vehicles(self, *, sold: bool | None = None):
        with self._lock:
            data = self._read()
            vehicles = list(data['vehicles'])
        if sold is not None:
            vehicles = [v for v in vehicles if bool(v.get('sold')) is bool(sold)]
        vehicles.sort(key=lambda v: v.get('created_at') or datetime.min, reverse=True)
        return vehicles

    def list_sold_vehicles(self):
        vehicles = self.list_vehicles(sold=True)
        vehicles.sort(key=lambda v: v.get('sold_at') or datetime.min, reverse=True)
        return vehicles

    def get_vehicle(self, vehicle_id: str):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    return v
        return None

    def create_vehicle(self, doc: dict):
        with self._lock:
            data = self._read()
            vehicle_id = uuid.uuid4().hex
            new_doc = dict(doc)
            new_doc['id'] = vehicle_id
            data['vehicles'].append(new_doc)
            self._write(data)
        return InsertResult(inserted_id=vehicle_id)

    def push_vehicle_expense(self, vehicle_id: str, expense: dict):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    v.setdefault('expenses', [])
                    v['expenses'].append(expense)
                    self._write(data)
                    return True
        return False

    def push_vehicle_document(self, vehicle_id: str, document: dict):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    v.setdefault('documents', [])
                    v['documents'].append(document)
                    self._write(data)
                    return True
        return False

    def set_vehicle_sold(self, vehicle_id: str, *, sale_id: str):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    v['sold'] = True
                    v['category'] = 'sold'
                    v['sold_at'] = _utcnow()
                    v['sale_id'] = sale_id
                    self._write(data)
                    return True
        return False

    def set_vehicle_category(self, vehicle_id: str, *, category: str):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    v['category'] = category
                    self._write(data)
                    return True
        return False

    def move_vehicle_to_inventory(self, vehicle_id: str, *, vat_paid_amount: float, paid_at):
        with self._lock:
            data = self._read()
            for v in data['vehicles']:
                if v.get('id') == vehicle_id:
                    v['category'] = 'inventory'
                    v['vat_paid_on_inventory'] = round(float(vat_paid_amount or 0), 2)
                    v['vat_paid_on_inventory_at'] = paid_at
                    self._write(data)
                    return True
        return False

    # Sales
    def list_sales(self, *, limit: int | None = None):
        with self._lock:
            data = self._read()
            sales = list(data['sales'])
        sales.sort(key=lambda s: s.get('created_at') or datetime.min, reverse=True)
        if limit is not None:
            sales = sales[:limit]
        return sales

    def get_sale(self, sale_id: str):
        with self._lock:
            data = self._read()
            for s in data['sales']:
                if s.get('id') == sale_id:
                    return s
        return None

    def create_sale(self, doc: dict):
        with self._lock:
            data = self._read()
            sale_id = uuid.uuid4().hex
            new_doc = dict(doc)
            new_doc['id'] = sale_id
            data['sales'].append(new_doc)
            self._write(data)
        return InsertResult(inserted_id=sale_id)

    def push_tax_payment(self, sale_id: str, payment: dict):
        with self._lock:
            data = self._read()
            for s in data['sales']:
                if s.get('id') == sale_id:
                    s.setdefault('tax_payments', [])
                    s['tax_payments'].append(payment)
                    self._write(data)
                    return True
        return False

    def sales_between(self, start: datetime, end: datetime):
        with self._lock:
            data = self._read()
            sales = list(data['sales'])
        out = []
        for s in sales:
            created_at = s.get('created_at')
            if created_at and start <= created_at < end:
                out.append(s)
        return out

    def sales_with_payments_between(self, start: datetime, end: datetime):
        with self._lock:
            data = self._read()
            sales = list(data['sales'])
        out = []
        for s in sales:
            payments = s.get('tax_payments') or []
            if any((p.get('paid_at') and start <= p['paid_at'] < end) for p in payments):
                out.append(s)
        return out

