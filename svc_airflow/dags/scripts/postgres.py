# Script to help interact with Postgres

import psycopg

def connect(host="db_main", port= "5433", dbname= 'dental_clinic', user= 'postgres', password= 'postgres'):
    conn = psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    return conn

def execute(query, values=None):
    conn = connect()
    cur = conn.cursor()

    cur.execute(query, values)

    conn.commit()
    conn.close()

def fetch(query):
    conn = connect()
    cur = conn.cursor()

    cur.execute(query)
    row = cur.fetchall()

    cur.close()
    return row

def table_check():
    table_patients = """
    CREATE TABLE IF NOT EXISTS patients (
        patient_id INTEGER PRIMARY KEY,
        complaint TEXT,
        first_name VARCHAR(12),
        last_name VARCHAR(12),
        date_of_birth DATE,
        address TEXT,
        phone VARCHAR(20)
    );
    """

    table_visits = """
    CREATE TABLE IF NOT EXISTS visits (
        visit_id INTEGER PRIMARY KEY,
        patient_id INTEGER,
        dentist_id INTEGER,
        cost INTEGER,
        payment_method VARCHAR(12),
        visit_day VARCHAR(12),
        visit_date DATE
    );
    """
    table_details = """
    CREATE TABLE IF NOT EXISTS visit_details (
        visit_id INTEGER PRIMARY KEY,
        diagnose VARCHAR(20),
        treatment VARCHAR(20),
        follow_up_required SMALLINT,
        note TEXT
    );
    """
    execute(table_patients)
    execute(table_visits)
    execute(table_details)

def data_check(table):
    if table == 'patients':
        query = """
        SELECT DISTINCT(patient_id) from patients;
        """
        results = fetch(query)
        return results
    
    elif table == 'visits':
        id_list = """
        SELECT DISTINCT(visit_id) from visits;
        """
        fetch_patient = """
        SELECT DISTINCT(patient_id) from patients;
        """

        results = fetch(id_list)
        patient_fetch = fetch(fetch_patient)

        return results, patient_fetch
    
    elif table == 'details':
        query = """
        SELECT DISTINCT(visit_id) from visit_details;
        """
        results = fetch(query)
        return results

    else:
        print("Can't find the table name")

def insertData_patients(new_patients, upd_patients):
    insertData_query = """
    INSERT INTO patients (patient_id, complaint, first_name, last_name, date_of_birth, address, phone)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    updateData_query = """
    UPDATE patients SET complaint = %s
    WHERE patient_id = %s
    """

    for data in new_patients:
        execute(insertData_query, (data['id'], data['complaint'], data['first_name'], data['last_name'],
                 data['date_of_birth'], data['address'], data['phone']))
        
        print(f"Succesfully added: {data['first_name']} {data['last_name']}")

    for data in upd_patients:
        execute(updateData_query, (data['complaint'], data['id']))
        
        print(f"Succesfully updated: {data['id']} {data['complaint']}")

def insertData_visits(visits):
    insertData_query = """
    INSERT INTO visits (visit_id, patient_id, dentist_id, cost, payment_method, visit_day, visit_date)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    for data in visits:
        execute(insertData_query, (data['id'], data['patient_id'], data['dentist_id'],
                 data['cost'], data['payment_method'], data['visit_day'], data['visit_date']))
        
        print(f"Succesfully added: {data['patient_id']} visits")

def insertData_details(details):
    insertData_query = """
    INSERT INTO visit_details (visit_id, diagnose, treatment, follow_up_required)
    VALUES (%s,%s,%s,%s)
    """
    for data in details:
        execute(insertData_query, (data['id'], data['diagnose'], data['treatment'],
                 data['follow_up']))
        
        print(f"Succesfully added: {data['id']} visits")