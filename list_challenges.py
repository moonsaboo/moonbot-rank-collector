# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')
import firebase_admin
from firebase_admin import credentials, firestore
from collect import SERVICE_ACCOUNT_KEY

cred = credentials.Certificate(SERVICE_ACCOUNT_KEY)
firebase_admin.initialize_app(cred)
db = firestore.client()

for ch in db.collection('challenges').stream():
    d = ch.to_dict()
    print(ch.id, '|', d.get('name'), '| start=', d.get('startDate'), '| end=', d.get('endDate'), '| totalDays=', d.get('totalDays'))

print('--- active ---')
active = db.collection('settings').document('active').get()
print(active.to_dict())

print('--- hall_of_fame existing ---')
for h in db.collection('hall_of_fame').stream():
    print(h.id, h.to_dict().get('challengeName'))
