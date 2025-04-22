# Data generator script for 'Dentist Database' : patients, visits, visit_details 
from faker import Faker
from datetime import datetime
import random

fke = Faker('id_ID')

def random_complaint():
    complaints = ['Gigi berlubang', 'Gusi bengkak', 'Karang gigi', 'Sakit gigi',
              'Gigi graham tumbuh', 'Gigi goyang', 'Gusi berdarah', 'Gigi sensitif',
              'Gigi patah', 'Gigi retak', 'Gigi copot', 'Gigi tumbuh miring', 'Luka di gusi',
              'Infeksi gigi']
    
    complaint = random.choice(complaints)

    return complaint

def custom_pekanbaru_address():
    district = [
    "Bukit Raya", "Lima Puluh", "Marpoyan Damai", "Payung Sekaki",
    "Pekanbaru Kota", "Rumbai", "Rumbai Barat", "Rumbai Timur",
    "Sail", "Senapelan", "Tampan", "Tenayan Raya"
]

    street = fke.street_name()
    number = fke.building_number()
    district = random.choice(district)
    city = "Pekanbaru"
    province = "Riau"
    postal = fke.postcode()
    return f"{street} No. {number}, Kec. {district}, {city}, {province} {postal}"

def generate_patients(patient_fetch):
    new_patients = []
    upd_patients = []
    exst_id = [row[0] for row in patient_fetch]
    max_id = len(exst_id)

    for i in range(5):
        data = {}
        id = random.randint(1, 200)

        if id in exst_id:
            data['id'] = id
            data['complaint'] = random_complaint()

            upd_patients.append(data)

        else:
            max_id += 1

            data['id'] = max_id
            data['complaint'] = random_complaint()
            data['first_name'] = fke.first_name()
            data['last_name'] = fke.last_name()
            data['date_of_birth'] = fke.date_of_birth(minimum_age=18, maximum_age=60).strftime('%Y-%m-%d')
            data['address'] = custom_pekanbaru_address()
            data['phone'] = fke.phone_number().replace(' ', '')

            new_patients.append(data)
    
    return new_patients, upd_patients

def generate_visits(id_list, patient_fetch):
    visits = []
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday']
    payments = ['Cash', 'Qris', 'Debit', 'Transfer']

    for i in range (10):
        data = {}

        randpatient = random.choice(patient_fetch)
        
        date_time = fke.date_time_between(start_date=datetime(2025,1,1), end_date=datetime(2025,4,22))
        hari = date_time.strftime('%A')
        tanggal = date_time.strftime('%Y-%m-%d')


        data['id'] = id_list + i + 1
        data['patient_id'] = randpatient[0]
        if hari in weekdays:
            data['dentist_id'] = data['dentist_id'] = random.choice([1, 2])
        else:
            data['dentist_id'] = data['dentist_id'] = random.choice([3, 4])
        data['cost'] = random.randint(150000, 1000000)
        data['payment_method'] = random.choice(payments)
        data['visit_day'] = hari
        data['visit_date'] = tanggal

        visits.append(data)
    
    return visits

def random_DiagnoseAndTreatment():
    DiagnosesAndTreatments = [
        ['Gigi berlubang', 'Tambal gigi'],
        ['Karang gigi', 'Scaling'],
        ['Radang gusi', 'Scaling'],
        ['Sakit akar gigi', 'Cabut gigi'],
        ['Infeksi gusi', 'Antibiotik'],
        ['Gigi sensitif', 'Fluoride'],
        ['Gigi bungsu impaksi', 'Cabut gigi'],
        ['Gigi retak', 'Tambal gigi'],
        ['Bruxism', 'Mouthguard'],
        ['Maloklusi', 'Behel']
    ]
    
    diagnose, treatment = random.choice(DiagnosesAndTreatments)
    return diagnose, treatment

def generate_details(id_list):
    details = []

    for i in range (10):
        data = {}
        diagnose, treatment = random_DiagnoseAndTreatment()

        data['id'] = id_list + i + 1
        data['diagnose'] = diagnose
        data['treatment'] = treatment
        data['follow_up'] = random.choice([0,1])
        details.append(data)
    
    return details