#!/usr/bin/env python3
import json
import os
import uuid
from datetime import datetime

DATA_FILE = 'sales.json'

def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def input_with_default(prompt, default):
    entry = input(f"{prompt} [{default}]: ")
    return entry.strip() or default

def input_sale_details(existing=None):
    existing = existing or {}
    sale = {}
    sale['id'] = existing.get('id') or str(uuid.uuid4())
    sale['date'] = existing.get('date') or datetime.now().isoformat()
    sale['buyer'] = input_with_default('Buyer name', existing.get('buyer',''))
    sale['phone'] = input_with_default('Buyer phone', existing.get('phone',''))
    sale['make'] = input_with_default('Car make', existing.get('make',''))
    sale['model'] = input_with_default('Car model', existing.get('model',''))
    sale['year'] = input_with_default('Year', existing.get('year',''))
    sale['color'] = input_with_default('Color', existing.get('color',''))
    price_str = input_with_default('Price (numeric)', str(existing.get('price','0')))
    try:
        sale['price'] = float(price_str)
    except ValueError:
        sale['price'] = 0.0
    tax_str = input_with_default('Tax rate %', str(existing.get('tax_rate',10)))
    try:
        sale['tax_rate'] = float(tax_str)
    except ValueError:
        sale['tax_rate'] = 0.0
    sale['payment_method'] = input_with_default('Payment method', existing.get('payment_method','Cash'))
    sale['notes'] = input_with_default('Notes', existing.get('notes',''))
    sale['total'] = round(sale['price'] * (1 + sale['tax_rate']/100), 2)
    return sale

def print_receipt(sale):
    lines = []
    lines.append('+' + '-'*46 + '+')
    lines.append('|{:^46}|'.format('CAR SALE RECEIPT'))
    lines.append('+' + '-'*46 + '+')
    lines.append(f"Date: {sale.get('date')}")
    lines.append(f"Receipt ID: {sale.get('id')}")
    lines.append('-'*48)
    lines.append(f"Buyer: {sale.get('buyer')}    Phone: {sale.get('phone')}")
    lines.append(f"Car: {sale.get('year')} {sale.get('make')} {sale.get('model')} ({sale.get('color')})")
    lines.append(f"Price: ${sale.get('price'):.2f}")
    lines.append(f"Tax rate: {sale.get('tax_rate'):.2f}%")
    lines.append(f"Total: ${sale.get('total'):.2f}")
    lines.append(f"Payment: {sale.get('payment_method')}")
    if sale.get('notes'):
        lines.append('-'*48)
        lines.append('Notes:')
        lines.append(sale.get('notes'))
    lines.append('+' + '-'*46 + '+')
    print('\n'.join(lines))

def list_sales(data):
    if not data:
        print('No sales recorded yet.')
        return
    for i, s in enumerate(data,1):
        print(f"{i}. {s.get('buyer','<no buyer>')} - {s.get('make','')} {s.get('model','')} ({s.get('id')})")

def find_sale_by_id(data, sale_id):
    for s in data:
        if s.get('id') == sale_id:
            return s
    return None

def main():
    data = load_data()
    while True:
        print('\nCar Sale System')
        print('1) New sale')
        print('2) Edit sale')
        print('3) List sales')
        print('4) View receipt')
        print('5) Exit')
        choice = input('Choose an option: ').strip()
        if choice == '1':
            sale = input_sale_details()
            data.append(sale)
            save_data(data)
            print('\nSaved. Receipt:')
            print_receipt(sale)
        elif choice == '2':
            list_sales(data)
            sid = input('Enter receipt ID to edit: ').strip()
            s = find_sale_by_id(data, sid)
            if not s:
                print('Sale not found.')
                continue
            updated = input_sale_details(existing=s)
            # replace in list
            for i, item in enumerate(data):
                if item.get('id') == sid:
                    data[i] = updated
                    break
            save_data(data)
            print('Updated. New receipt:')
            print_receipt(updated)
        elif choice == '3':
            list_sales(data)
        elif choice == '4':
            list_sales(data)
            sid = input('Enter receipt ID to view: ').strip()
            s = find_sale_by_id(data, sid)
            if not s:
                print('Sale not found.')
                continue
            print_receipt(s)
        elif choice == '5':
            print('Goodbye')
            break
        else:
            print('Invalid choice')

if __name__ == '__main__':
    main()
