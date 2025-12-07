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
    #LEGEND: need_id, company_id, salary, category_id, category(name), subcategory_id, subcategory(name), city_id, city(name) latitude, longitude, schedule_id, schedule_name
    query = """
    SELECT n.id as need_id, c.id AS company_id, n.salary, ctg.id AS category_id, ctg.value AS category, 
    s.id AS subcategory_id, s.value AS subcategory, cty.id AS city_id, cty.name AS city, cty.lat as latitude, cty.lng as longitude, sch.id as schedule_id, sch.value AS schedule_name
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


def getCandidates(need):
    need_id = need.need_id
    company_id = need.company_id
    category_id = need.category_id
    subcategory_id = need.subcategory_id
    city_id = need.city_id
    if subcategory_id == 128 or subcategory_id == 129:
        subcategory_id = None
    #LEGEND: candidate_id, salary_preference, experience_id, experience, education_id, education, category_id, category, subcategory_id, subcategory, city_id, city
    query = """
    SELECT c.id AS candidate_id , c.desired_salary AS salary_preference, c.experience_id, exp.value AS experience, c.education_id, edu.value AS education,
    cde.category_id AS category_id, ctg.value AS category, cde.subcategory_id AS subcategory_id, sctg.value AS subcategory, ccty.city_id, cty.name as city
    FROM candidates c
    INNER JOIN education edu ON edu.id = c.education_id
    INNER JOIN experiences exp ON exp.id = c.experience_id
    INNER JOIN candidate_domain_experiences cde ON cde.candidate_id = c.id
    INNER JOIN candidate_city ccty ON c.id = ccty.candidate_id
    INNER JOIN cities cty ON ccty.city_id = cty.id AND ccty.city_id = :cityId
    INNER JOIN categories ctg ON cde.category_id = ctg.id
    INNER JOIN subcategories sctg ON cde.subcategory_id = sctg.id 

    WHERE    /*  EXCLUSIONS  */
        c.current_employer IS NULL
        AND NOT EXISTS ( 
            SELECT 1
            FROM candidate_blocked_companies blc
            WHERE blc.candidate_id = c.id
            AND blc.company_id = :companyId
        )
        AND NOT EXISTS (
            SELECT 1
            FROM processes prc
            WHERE prc.candidate_id = c.id
            AND prc.need_id = :needId
        )
        /*  INCLUSIONS  */
        AND (cde.category_id = :categoryId AND cde.subcategory_id = :subcategoryId)

    ORDER BY c.education_id DESC, c.desired_salary ASC, c.id;
    """
    result = conn.execute(text(query), {"needId": need_id, "companyId":company_id, "categoryId":category_id, "subcategoryId":subcategory_id, "cityId":city_id})

    return result

need = getNeedData(2454).all()

candidates = getCandidates(need[0]).all()

print(need[0])

print(candidates)

