from datetime import datetime, timedelta
import pytz
import requests
import time
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import traceback
import mysql.connector

user = "root"
password = "<password>"
host = "localhost"
port = "3306"
database_duomenys = "raw_data"
database_apdorojimas = "transformed_data"


def get_new_day_data():
    
    
    # Define the Lithuania timezone
    lithuania_tz = pytz.timezone('Europe/Vilnius')

    # Get current time in Lithuania
    now_in_lithuania = datetime.now(lithuania_tz)

    # Subtract one day to get yesterday
    yesterday = now_in_lithuania - timedelta(days=1)

    year = yesterday.year
    month = yesterday.month
    day = yesterday.day



    # Drop previous new_day_data
    conn = mysql.connector.connect(
    host = host,       
    user = user,   
    password = password,
    database = database_duomenys
    )

    cursor = conn.cursor()

    
    cursor.execute("DROP TABLE IF EXISTS new_day_data")

   
    conn.commit()
    cursor.close()
    conn.close()




    # Get new_day_data
    session = requests.Session()


    login_url = "https://<url.lt>/auth/login"
    login_data = {
        "email": "<user@email.com>",
        "password": "<userpassword>"
    }

    login_response = session.post(login_url, data=login_data)

    if login_response.status_code != 200 or "logout" not in login_response.text.lower():
        print("Login failed")
        print(login_response.text)
        exit()

    print("Login successful!")

    token_url = "https://<url.lt>/api/getJWT"

    duomenys_database = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_duomenys}")
   
    request_url = "https://<url.lt>/api/v1/reports?"


    start_date = datetime(year, month, day)
    end_date = datetime(year, month, day)

    delta = timedelta(days=1)


    while start_date <= end_date:
        try:
            token_response = session.get(token_url)

            token = token_response.json()["JWT"]
            headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Authorization": f"Bearer {token}",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://<url.lt>/admin/reports",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/136.0.0.0 Safari/537.36"
            }

            date_str = start_date.strftime("%Y-%m-%d")

            params = {
                "shops": "",
                "clients": "",
                "items": "",
                "cards": "",
                "shop_id": "",
                "client_id": "",
                "item_id": "",
                "card_id": "",
                "apps_id": "",
                "superviser_id": "",
                "card_type": "",
                "type": "nogroup",
                "from":date_str,
                "to": date_str,
                "_search": "false",
                "nd": str(int(time.time() * 1000)),
                "rows": "100",
                "page": "1",
                "sidx": "",
                "sord": "asc"
            }

            
            report_response = session.get(request_url, headers=headers, params=params)

            print(report_response.status_code)
            

            report_json = report_response.json()

            
            if "data" not in report_json or not isinstance(report_json["data"], list):
                print("No valid data found in response")
                exit()

           
            records = []
            for record in report_json["data"]:
                records.append({
                    "id": record.get("id"),
                    "item": record.get("item"),
                    "provided_name": record.get("provided_name"),
                    "provided_group": record.get("provided_group"),
                    "TID": record.get("TID"),
                    "qt": record.get("qt"),
                    "created_at": record.get("created_at"),
                    "updated_at": record.get("updated_at"),
                    "pvm": record.get("pvm"),
                    "unit": record.get("unit"),
                    "pvmValue": record.get("pvmValue"),
                    "total": record.get("total"),
                    "commited": record.get("commited"),
                    "pos_code": record.get("pos_code"),
                    "client_id": record.get("client_id"),
                    "card_number": record.get("card_number"),
                    "tdate": record.get("tdate"),
                    "bday": record.get("bday"),
                    "balance": record.get("balance"),
                    "tpayed": record.get("tpayed"),
                    "tcharge": record.get("tcharge"),
                    "checkid": record.get("checkid"),
                    "details": record.get("details"),
                    "credit": record.get("credit"),
                    "debit": record.get("debit"),
                    "storn": record.get("storn"),
                    "status": record.get("status"),
                    "commited_local": record.get("commited_local"),
                    "roaming_sp": record.get("roaming_sp"),
                    "external": record.get("external"),
                    "partner": record.get("partner"),
                    "atotal": record.get("atotal"),
                    "number": record.get("number"),
                    "type": record.get("type"),
                    "pin": record.get("pin"),
                    "block_reason": record.get("block_reason"),
                    "valid_from": record.get("valid_from"),
                    "valid_to": record.get("valid_to"),
                    "rvalue": record.get("rvalue"),
                    "day_credit_limit": record.get("day_credit_limit"),
                    "day_credit": record.get("day_credit"),
                    "credit_allowance": record.get("credit_allowance"),
                    "credit_limit": record.get("credit_limit"),
                    "fcredit": record.get("fcredit"),
                    "notes": record.get("notes"),
                    "countLimit": record.get("countLimit"),
                    "countLimitDay": record.get("countLimitDay"),
                    "name": record.get("name"),
                    "service_provider_id": record.get("service_provider_id"),
                    "manager_id": record.get("manager_id"),
                    "superviser_id": record.get("superviser_id"),
                    "unlockop": record.get("unlockop"),
                    "payday": record.get("payday"),
                    "debt": record.get("debt"),
                    "debt_covered": record.get("debt_covered"),
                    "debt_allowance": record.get("debt_allowance"),
                    "protocol_sw_s": record.get("protocol_sw_s"),
                    "protocol_sw_w": record.get("protocol_sw_w"),
                    "protocol_bonus": record.get("protocol_bonus"),
                    "unlockvalue": record.get("unlockvalue"),
                    "phone": record.get("phone"),
                    "phone2": record.get("phone2"),
                    "city": record.get("city"),
                    "fax": record.get("fax"),
                    "email": record.get("email"),
                    "address": record.get("address"),
                    "vat_number": record.get("vat_number"),
                    "activity": record.get("activity"),
                    "contact_person": record.get("contact_person"),
                    "contact_person_phone": record.get("contact_person_phone"),
                    "bank": record.get("bank"),
                    "bank_code": record.get("bank_code"),
                    "account_num": record.get("account_num"),
                    "code": record.get("code"),
                    "password": record.get("password"),
                    "shop_id": record.get("shop_id"),
                    "serial_num": record.get("serial_num"),
                    "price": record.get("price"),
                    "dprice": record.get("dprice"),
                    "rule_id": record.get("rule_id"),
                    "position": record.get("position"),
                    "pprice": record.get("pprice"),
                    "pprice_jud": record.get("pprice_jud"),
                    "order": record.get("order"),
                    "discount_plan_id": record.get("discount_plan_id"),
                    "formula": record.get("formula"),
                    "target": record.get("target"),
                    "condition": record.get("condition"),
                    "date_from": record.get("date_from"),
                    "date_to": record.get("date_to"),
                    "card_mask": record.get("card_mask"),
                    "properties": record.get("properties"),
                    "overprice": record.get("overprice"),
                    "protocol_type": record.get("protocol_type"),
                    "user": record.get("user"),
                    "item_code": record.get("item_code"),
                    "incorrect": record.get("incorrect"),
                    "client_overprice": record.get("client_overprice"),
                    "protocol_full": record.get("protocol_full"),
                    "protocol": record.get("protocol"),
                    "cname": record.get("cname"),
                    "ccode": record.get("ccode"),
                    "superviser_name": record.get("superviser_name"),
                    "ctype": record.get("ctype"),
                    "shop": record.get("shop"),
                    "date": record.get("date"),
                    "trid": record.get("trid"),
                    "usedplan": record.get("usedplan"),
                    "product_code": record.get("product_code"),
                    "product_name": record.get("product_name"),
                    "price_single": record.get("price_single"),
                    "price_total": record.get("price_total"),
                    "paid": record.get("paid"),
                    "discount_total": record.get("discount_total"),
                    "discount_single": record.get("discount_single"),
                    "partner_protocol": record.get("partner_protocol"),
                    "partner_to": record.get("partner_to")
                })
                
                

            report_df = pd.DataFrame(records)


            report_df.to_sql(name="new_day_data", con=duomenys_database, if_exists='append', index=False)
            print(f"Data for {date_str} written to database.")

            start_date += delta
        except Exception as e:
            print(f"An error occurred on {start_date.strftime('%Y-%m-%d')}: {str(e)}")
            session = requests.Session()


    
            login_response = session.post(login_url, data=login_data)

            if login_response.status_code != 200 or "logout" not in login_response.text.lower():
                print("Login failed")
                print(login_response.text)
                exit()

            print("Login successful!")




def delete_duplicates():

    
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_duomenys
    )
    cursor = conn.cursor()

   
    cursor.execute("""
    CREATE TABLE temp_table AS 
    SELECT DISTINCT * FROM new_day_data;
    """)

   
    cursor.execute("DELETE FROM new_day_data;")

   
    cursor.execute("""
    INSERT INTO new_day_data 
    SELECT * FROM temp_table;
    """)

   
    cursor.execute("DROP TABLE temp_table;")

    conn.commit()
    cursor.close()
    conn.close()




def append_new_day_data_to_total_data():

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database_duomenys}')

    query = """
    INSERT INTO total_data
        SELECT * FROM new_day_data
    """

   
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit() 



def select_required_columns_to_new_day_data_1():

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database_duomenys}')

    
    query = """
    SELECT item, provided_name, qt, city, address, shop, date, paid
    FROM duomenys.new_day_data
    """
    df = pd.read_sql(query, engine)

   
    table_apdorojimas = 'new_day_data_1'

    
    engine_apdorojimas = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}')

    
    df.to_sql(table_apdorojimas, con=engine_apdorojimas, index=False, if_exists='replace')




def add_column_preke():
   
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    
    item_values = [
        "1", "2", "3", "000000000003", "000000000004", "4", "5", "6", 
        "000000000008", "8", "000000000013", "000000000015", "16",
        "f_1", "f_2", "f_3", "f_4", "f_5"
    ]
    formatted_values = ",".join(f"'{v}'" for v in item_values)

    
    check_column_sql = """
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'new_day_data_1' AND COLUMN_NAME = 'preke' AND TABLE_SCHEMA = :schema;
    """

    add_column_sql = """
    ALTER TABLE new_day_data_1
    ADD COLUMN preke text;
    """

    update_sql = f"""
    UPDATE new_day_data_1
    SET preke = 'main product'
    WHERE item IN ({formatted_values});
    """

   
    with engine.begin() as conn:
        result = conn.execute(text(check_column_sql), {"schema": database_apdorojimas})
        column_exists = result.scalar() > 0

        if not column_exists:
            conn.execute(text(add_column_sql))
            print("✅ Column 'preke' added.")
        else:
            print("ℹ️ Column 'preke' already exists.")

        conn.execute(text(update_sql))
        print("✅ Column 'preke' updated where item matched.")



def add_kt_prekes_to_preke():
   
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    
    item_values = [
        "1", "2", "3", "000000000003", "000000000004", "4", "5", "6", 
        "000000000008", "8", "000000000013", "000000000015", "16",
        "f_1", "f_2", "f_3", "f_4", "f_5"
    ]
    formatted_values = ",".join(f"'{v}'" for v in item_values)


    update_sql = f"""
    UPDATE new_day_data_1
    SET preke = 'other products'
    WHERE item NOT IN ({formatted_values});
    """

    
    with engine.begin() as conn:
        conn.execute(text(update_sql))
        




def add_column_metai_menuo():

    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    with engine.begin() as conn:
        
        result = conn.execute(text("""
            SHOW COLUMNS FROM new_day_data_1 LIKE 'metai_menuo';
        """))
        if result.first() is None:
            
            conn.execute(text("""
                ALTER TABLE new_day_data_1
                ADD COLUMN metai_menuo VARCHAR(7);
            """))

        
        conn.execute(text("""
            UPDATE new_day_data_1
            SET metai_menuo = LEFT(DATE_FORMAT(date, '%Y-%m-%d'), 7);
        """))

        print("✅ Column 'metai_menuo' added and populated.")



def change_qt_to_float():
    
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_apdorojimas
    )

    cursor = conn.cursor()


   
    try:
        cursor.execute("ALTER TABLE new_day_data_1 MODIFY COLUMN qt FLOAT")
        print("Column 'qt' successfully converted to FLOAT.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")

   
    conn.commit()
    cursor.close()
    conn.close()      





def change_paid_to_float():
    
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_apdorojimas
    )

    cursor = conn.cursor()


   
    try:
        cursor.execute("ALTER TABLE new_day_data_1 MODIFY COLUMN paid FLOAT")
        print("Column 'paid' successfully converted to FLOAT.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")

   
    conn.commit()
    cursor.close()
    conn.close()   



def add_column_pardavejas():
   
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    
    item_values = [
        <items>
    ]
    formatted_values = ",".join(f"'{v}'" for v in item_values)

    
    check_column_sql = """
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'new_day_data_1' AND COLUMN_NAME = 'pardavejas' AND TABLE_SCHEMA = :schema;
    """

    add_column_sql = """
    ALTER TABLE new_day_data_1
    ADD COLUMN pardavejas text;
    """

    update_sql = f"""
    UPDATE new_day_data_1
    SET pardavejas = 'main seller'
    WHERE shop IN ({formatted_values});
    """

    update_sql_1 = f"""
    UPDATE new_day_data_1
    SET pardavejas = 'partner'
    WHERE shop NOT IN ({formatted_values});
    """

    
    with engine.begin() as conn:
        result = conn.execute(text(check_column_sql), {"schema": database_apdorojimas})
        column_exists = result.scalar() > 0

        if not column_exists:
            conn.execute(text(add_column_sql))
            print("✅ Column 'pardavejas' added.")
        else:
            print("ℹ️ Column 'pardavejas' already exists.")

        conn.execute(text(update_sql))
        conn.execute(text(update_sql_1))
        


def add_column_metai():

    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    with engine.begin() as conn:
       
        result = conn.execute(text("""
            SHOW COLUMNS FROM new_day_data_1 LIKE 'metai';
        """))
        if result.first() is None:
           
            conn.execute(text("""
                ALTER TABLE new_day_data_1
                ADD COLUMN metai VARCHAR(4);
            """))

        
        conn.execute(text("""
            UPDATE new_day_data_1
            SET metai = LEFT(DATE_FORMAT(date, '%Y-%m-%d'), 4);
        """))

        print("✅ Column 'metai' added and populated.")




def add_column_month():

    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    with engine.begin() as conn:
       
        result = conn.execute(text("""
            SHOW COLUMNS FROM new_day_data_1 LIKE 'month';
        """))
        if result.first() is None:
            
            conn.execute(text("""
                ALTER TABLE new_day_data_1
                ADD COLUMN month VARCHAR(2);
            """))

        conn.execute(text("""
            UPDATE new_day_data_1
            SET month = DATE_FORMAT(date, '%m');
        """))

        print("✅ Column 'month' added and populated.")




def add_column_date_1():

    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}")

    with engine.begin() as conn:
       
        result = conn.execute(text("""
            SHOW COLUMNS FROM new_day_data_1 LIKE 'date_1';
        """))
        if result.first() is None:
           
            conn.execute(text("""
                ALTER TABLE new_day_data_1
                ADD COLUMN date_1 DATETIME;
            """))

       
        conn.execute(text("""
            UPDATE new_day_data_1
            SET date_1 = DATE(`date`);
        """))

        print("✅ Column 'date_1' added and populated.")





def append_new_day_data_1_to_duomenys_1():

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database_apdorojimas}')

   
    query = """
    INSERT INTO duomenys_1
        SELECT * FROM new_day_data_1
    """

   
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit() 



get_new_day_data()
delete_duplicates()
append_new_day_data_to_total_data()
select_required_columns_to_new_day_data_1()
add_column_preke()
add_column_metai_menuo()
change_qt_to_float()
change_paid_to_float()
add_kt_prekes_to_preke()
add_column_pardavejas()
add_column_metai()
add_column_month()
add_column_date_1()
append_new_day_data_1_to_duomenys_1()
