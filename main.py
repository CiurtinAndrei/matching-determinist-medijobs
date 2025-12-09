from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import pandas as pd
from pandas import DataFrame
from math import *
import os
import io

load_dotenv()
try:
    mysql_engine = create_engine(f"mysql+pymysql://{os.getenv('USER')}:{os.getenv('PASS')}@localhost:{os.getenv('PORT')}/{os.getenv('DBNAME')}")
    conn = mysql_engine.connect()
    print("MySQL connection is successful!")
except SQLAlchemyError as e:
    print("MySQL connection failed!" + e)


def haversineDistance(lat1, lon1, lat2, lon2):
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    r = 6371

    return (c * r)


def getAvailableNeeds():
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
    WHERE n.active = 1 AND n.signed = 1 AND n.finalized_status IS NULL;
    """
    result = conn.execute(text(query))
    return result

def getNeedData(id):
    #TEST CASES: 2454 (cazul fericit), 9543 (cazul cu prea putini candidati in orasul respectiv), 8013 (caz fericit - asistenti), 9353 (prea putini candidati: domeniu obscur)
    #TEST CASES: 9717 (caz intr-un oras in afara de bucuresti. TREBUIE NEAPARAT DIN ALTE ORASE!!!!), 10195 (iar prea putini, dar le trebuie garzi!)
    #LEGEND: need_id, company_id, salary, category_id, category(name), subcategory_id, subcategory(name), city_id, city(name), county, latitude, longitude, schedule_id, schedule_name
    query = """
    SELECT n.id as need_id, c.id AS company_id, n.salary, ctg.id AS category_id, ctg.value AS category, 
    s.id AS subcategory_id, s.value AS subcategory, cty.id AS city_id, cty.name AS city, cty.county, cty.lat as latitude, cty.lng as longitude, 
    sch.id as schedule_id, sch.value AS schedule_name
    FROM needs n
    INNER JOIN companies c ON (n.company_id = c.id)
    INNER JOIN categories ctg ON(n.category_id = ctg.id)
    INNER JOIN subcategories s ON(n.subcategory_id = s.id)
    INNER JOIN cities cty ON(n.city_id = cty.id)
    INNER JOIN need_schedule ns ON(n.id = ns.need_id)
    INNER JOIN schedules sch ON (ns.schedule_id = sch.id)
    WHERE n.active = 1 AND n.signed = 1 AND n.finalized_status IS NULL AND n.id = :id
    ORDER BY schedule_id DESC;
    """
    result = conn.execute(text(query), {"id": id})
    return result


def getCandidates(need):
    need_id = need.need_id
    company_id = need.company_id
    category_id = need.category_id
    subcategory_id = need.subcategory_id    
    city_id = need.city_id
    county = need.county
    if subcategory_id == 128 or subcategory_id == 129:
        subcategory_id = None
    #LEGEND: candidate_id, salary_preference, experience_id, experience, education_id, education, category_id, category, subcategory_id, subcategory, city_id, city, county
    query = """
    SELECT DISTINCT c.id AS candidate_id , c.desired_salary AS salary_preference, c.experience_id, exp.value AS experience, c.education_id, edu.value AS education,
    cde.category_id AS category_id, ctg.value AS category, cde.subcategory_id AS subcategory_id, sctg.value AS subcategory, ccty.city_id, cty.name as city, cty.county
    FROM candidates c
    INNER JOIN education edu ON edu.id = c.education_id
    INNER JOIN experiences exp ON exp.id = c.experience_id
    INNER JOIN candidate_domain_experiences cde ON cde.candidate_id = c.id
    INNER JOIN candidate_city ccty ON c.id = ccty.candidate_id 
    INNER JOIN cities cty ON ccty.city_id = cty.id
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
        AND (cde.category_id = :categoryId AND cde.subcategory_id = :subcategoryId AND c.identification_id <= 2) AND (cty.id = :cityId)

    ORDER BY c.education_id DESC, c.experience_id DESC, c.desired_salary ASC, c.id;
    """
    result = conn.execute(text(query), {"needId": need_id, "companyId":company_id, "categoryId":category_id, "subcategoryId":subcategory_id, "cityId":city_id, "county":county})

    return result



def getVicinityCandidates(need, counties):
    need_id = need.need_id
    company_id = need.company_id
    category_id = need.category_id
    subcategory_id = need.subcategory_id    
    city_id = need.city_id
    county = need.county
    if subcategory_id == 128 or subcategory_id == 129:
        subcategory_id = None
    query = """
    SELECT DISTINCT c.id AS candidate_id , c.desired_salary AS salary_preference, c.experience_id, exp.value AS experience, c.education_id, edu.value AS education,
    cde.category_id AS category_id, ctg.value AS category, cde.subcategory_id AS subcategory_id, sctg.value AS subcategory, ccty.city_id, cty.name as city, cty.county
    FROM candidates c
    INNER JOIN education edu ON edu.id = c.education_id
    INNER JOIN experiences exp ON exp.id = c.experience_id
    INNER JOIN candidate_domain_experiences cde ON cde.candidate_id = c.id
    INNER JOIN candidate_city ccty ON c.id = ccty.candidate_id 
    INNER JOIN cities cty ON ccty.city_id = cty.id
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
        AND (cde.category_id = :categoryId AND cde.subcategory_id = :subcategoryId AND c.identification_id <= 2) AND (cty.county = :county
    """

    for row in counties:
        query = query + f" OR cty.county = '{row.county}'"
    query = query + ") "
    query = query + "ORDER BY c.education_id DESC, c.experience_id DESC, c.desired_salary ASC, c.id;"

    result = conn.execute(text(query), {"needId": need_id, "companyId":company_id, "categoryId":category_id, "subcategoryId":subcategory_id, "cityId":city_id, "county":county})

    unique = set()
    uniqueCandidates = []
    for row in result:
        if row.candidate_id not in unique:
            unique.add(row.candidate_id)
            uniqueCandidates.append(row)
    return uniqueCandidates


def getCandidatesBySchedule(need, schedules):
    need_id = need.need_id
    company_id = need.company_id
    category_id = need.category_id
    subcategory_id = need.subcategory_id    
    city_id = need.city_id
    county = need.county
    if subcategory_id == 128 or subcategory_id == 129:
        subcategory_id = None
    
    query = """
    SELECT DISTINCT c.id AS candidate_id , c.desired_salary AS salary_preference, c.experience_id, exp.value AS experience, c.education_id, edu.value AS education,
    cde.category_id AS category_id, ctg.value AS category, cde.subcategory_id AS subcategory_id, sctg.value AS subcategory, ccty.city_id, cty.name as city, cty.county
    FROM candidates c
    INNER JOIN education edu ON edu.id = c.education_id
    INNER JOIN experiences exp ON exp.id = c.experience_id
    INNER JOIN candidate_domain_experiences cde ON cde.candidate_id = c.id
    INNER JOIN candidate_city ccty ON c.id = ccty.candidate_id 
    INNER JOIN cities cty ON ccty.city_id = cty.id
    INNER JOIN categories ctg ON cde.category_id = ctg.id
    INNER JOIN subcategories sctg ON cde.subcategory_id = sctg.id
    INNER JOIN candidate_schedule csch ON c.id = csch.candidate_id

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
        AND (cde.category_id = :categoryId AND cde.subcategory_id = :subcategoryId AND c.identification_id <= 2) AND (cty.id = :cityId) AND (csch.schedule_id = 50 
    """

    for schedule in schedules:
        query = query + f" OR csch.schedule_id = {schedule}"
    query = query + ") "
    query = query + "ORDER BY c.education_id DESC, c.experience_id DESC, c.desired_salary ASC, c.id;"

    result = conn.execute(text(query), {"needId": need_id, "companyId":company_id, "categoryId":category_id, "subcategoryId":subcategory_id, "cityId":city_id, "county":county})

    unique = set()
    uniqueCandidates = []
    for row in result:
        if row.candidate_id not in unique:
            unique.add(row.candidate_id)
            uniqueCandidates.append(row)
    return uniqueCandidates

def getAllCounties():

    query = """
    SELECT id, county, lat, lng
    FROM cities
    WHERE county != "Altele"
    ORDER BY county ASC;
    """
    result = conn.execute(text(query)).all()
    unique = set()
    uniqueCounties = []
    for row in result:
        if row.county not in unique:
            unique.add(row.county)
            uniqueCounties.append(row)
    return uniqueCounties


def getNeighbouringCounties(need):

    uniqueCounties = getAllCounties()
    
    treshold = 150 # Distance in Km
    validCounties = []
    for county in uniqueCounties:
        distance = haversineDistance(need.latitude, need.longitude, county.lat, county.lng)
        if (distance <= treshold):
            validCounties.append(county)
        
    return validCounties


def exportCandidateDataTxt(need, candidateList):
    filePath = f"./exports/need_{need.need_id}/candidates.txt"
    with io.open(filePath, "w", encoding='utf-8') as file:
        file.write(f"Number of unique candidates: {len(candidateList)} \n\n\n")
        for row in candidateList:
            file.write(str(row) + "\n")

def exportCandidateDataExcel(need, candidateList):
    filePath = f"./exports/need_{need.need_id}/candidates.xlsx"
    if not os.path.exists(f"./exports/need_{need.need_id}"):
        os.makedirs(f"./exports/need_{need.need_id}")
    data2D = []
    for row in candidateList:
        data2D.append(list(row))
    df = pd.DataFrame(data2D, columns = ['Candidate ID', 'Prefered Salary', 'Experience ID', 'Experience Level', 'Education ID', 'Education Level', 'Category ID', 'Category', 'Subcategory ID', 'Subcategory', 'City ID', 'City Name', 'County'])
    with pd.ExcelWriter(filePath) as writer:
        df.to_excel(writer)
    
    
def executeMatching(need_id):

    print(f"Starting deterministic matching process for need: {need_id}")

    need = getNeedData(need_id).all()
    if len(need) == 0:
        print(f"No valid need was found for id: {need_id}!")
        return

    candidates = getCandidates(need[0]).all()

    if len(candidates) == 0:
        print("No candidates found!")
        return
    

    if (len(candidates) < 50):
        print("Not enough candidates in the main county. Search has been expanded.")
        counties = getNeighbouringCounties(need[0])
        candidates = getVicinityCandidates(need[0], counties)

    if (len(candidates) < 100):
        print("Not enough candidates in the vicinity of the main county. Search has been expanded further.")
        counties = getAllCounties()
        candidates = getVicinityCandidates(need[0], counties)

    if(len(candidates) > 500):
        print("Too many candidates. Matching will be performed based on schedule.")
        schedulesList = []
        for row in need:
            schedulesList.append(row.schedule_id)
        candidates = getCandidatesBySchedule(need[0], schedulesList)
        

    exportCandidateDataExcel(need[0], candidates)
    print("Candidate list exported successfully for need: " + str(need[0].need_id))
    print("No. of candidates: " + str(len(candidates)))


executeMatching(10195)
#executeMatching(9543)
#executeMatching(10195)