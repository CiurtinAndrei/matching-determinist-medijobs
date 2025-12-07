from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

load_dotenv()
try:
    mysql_engine = create_engine(f"mysql+pymysql://{os.getenv('USER')}:{os.getenv('PASS')}@localhost:{os.getenv('PORT')}/{os.getenv('DBNAME')}")
    conn = mysql_engine.connect()
    print("MySQL connection is successful!")
except SQLAlchemyError as e:
    print("MySQL connection failed!" + e)


def getNeedData(id):
    query = """
    SELECT n.id as need_id, ns.schedule_id, sch.value AS schedule_type, n.salary, c.id AS company_id , c.name AS company_name, ctg.id AS ctg_id, 
    ctg.value AS category, s.id as sctg_id, s.value AS subcategory, 
    cty.id AS city_id, cty.name AS city_name, cty.lat as latitude, cty.lng as longitude
    FROM needs n
    INNER JOIN companies c ON (n.company_id = c.id)
    INNER JOIN categories ctg ON(n.category_id = ctg.id)
    INNER JOIN subcategories s ON(n.subcategory_id = s.id)
    INNER JOIN cities cty ON(n.city_id = cty.id)
    INNER JOIN need_schedule ns ON(n.id = ns.need_id)
    INNER JOIN schedules sch ON (ns.schedule_id = sch.id)
    WHERE n.active = 1 AND n.signed = 1 AND n.finalized_status IS NULL AND n.id = :id;
    """
    result = conn.execute(text(query), {"id": id})

    return result

res = getNeedData(2454)

rows = res.all()

print(rows[0].need_id)