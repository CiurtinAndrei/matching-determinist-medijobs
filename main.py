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


def getAvailableNeeds():
    #LEGEND: need_id, company_id, salary, category_id, category(name), subcategory_id, subcategory(name), city_id, city(name) latitude, longitude, schedule_id, schedule_name
    with open("./scripts/get_all_needs.txt", "r") as file:
        query = file.read()
    result = conn.execute(text(query))
    return result

def getNeedData(id):
    #LEGEND: need_id, company_id, salary, category_id, category(name), subcategory_id, subcategory(name), city_id, city(name), county, latitude, longitude, schedule_id, schedule_name
    with open("./scripts/get_need_data.txt", "r") as file:
        query = file.read()
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
    with open("./scripts/get_candidates.txt", "r") as file:
        query = file.read()
    result = conn.execute(text(query), {"needId": need_id, "companyId":company_id, "categoryId":category_id, "subcategoryId":subcategory_id, "cityId":city_id, "county":county})

    return result

def getAbroadWorkCandidates(need):
    need_id = need.need_id
    company_id = need.company_id
    category_id = need.category_id
    subcategory_id = need.subcategory_id    
    city_id = need.city_id
    county = need.county

    if subcategory_id == 128 or subcategory_id == 129:
        subcategory_id = None
        
    #LEGEND: candidate_id, salary_preference, experience_id, experience, education_id, education, category_id, category, subcategory_id, subcategory, city_id, city, county
    with open("./scripts/get_abroad_work_candidates.txt", "r") as file:
        query = file.read()
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

    with open("./scripts/get_vicinity_candidates.txt", "r") as file:
        query = file.read()

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
    df1 = pd.DataFrame(data2D, columns = ['Candidate ID', 'Prefered Salary', 'Experience ID', 'Experience Level', 
                                          'Education ID', 'Education Level', 'Category ID', 'Category', 'Subcategory ID', 
                                          'Subcategory', 'City ID', 'City Name', 'County'])
    df2 = pd.DataFrame([list(need)], columns = ['Need ID', 'Company ID', 'Salary', 'Category ID', 'Category', 
                                                'Subcategory ID', 'Subcategory', 'City ID', 'City Name', 'County', 
                                                'Latitude', 'Longitude', 'Schedule ID', 'Schedule Type'])
    with pd.ExcelWriter(filePath) as writer:
        df1.to_excel(writer, sheet_name = "Candidates")
        df2.to_excel(writer, sheet_name = "Need Data")

        
def executeMatching(need_id):

    print(f"Starting deterministic matching process for need: {need_id}")
    need = getNeedData(need_id).all()

    if need[0].city[0:14] == 'In strainatate':
        print("This need is for a foreign country! Searching candidates...")
        candidates = getAbroadWorkCandidates(need[0]).all()
        if len(candidates) == 0:
            print("No candidates found!")
            return
        print("Candidate list exported successfully!")
        print("No. of candidates: " + str(len(candidates)))
        exportCandidateDataExcel(need[0], candidates)
        return candidates
    
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

        
    exportCandidateDataExcel(need[0], candidates)
    print("Candidate list exported successfully!")
    print("No. of candidates: " + str(len(candidates)))
    return candidates







def reprogram(quizAnswer):

    map = {7:1, 6:2, 5:3, 4:4, 3:5, 2:6, 1:7}
    return map.get(quizAnswer)



def getQuizResults():

    with open("./scripts/get_quiz_answers.txt", "r") as file:
        query = file.read()
    quizzes = pd.DataFrame(conn.execute(text(query)).all(), columns=['candidate_id', 'question_number', 'value'])
    quizzes['value'] = quizzes['value'].astype(float)

    candidateData = []
    for candidate_id, group in quizzes.groupby('candidate_id'):
        values = group['value'].to_list()
        tempList = [
            candidate_id,
            sum(values[0:3]) / 21,
            sum(values[3:5]) / 14,
            sum(values[5:7]) / 14,
            sum(values[7:9]) / 14,
            sum(values[9:11]) / 14,
            sum(values[11:13]) / 14,
            sum(values[13:15]) / 14,
            sum(values[15:17]) / 14
        ]
        candidateData.append(tempList)

    filePath = "./exports/quizzes/quiz_data.xlsx"
    os.makedirs(os.path.dirname(filePath), exist_ok=True)
    df = pd.DataFrame(candidateData, columns=['Candidate ID', 'Emotional Stability', 'Altruism', 'Desire to Specialize', 
                                              'Communication and Relations', 'Stress Resistance', 
                                              'Anticipation and Flexibility', 'Discipline', 'Organization'])
    df.to_excel(filePath, sheet_name="Quiz Data per Candidate", index=False)



#candidates = executeMatching(10195)
#candidates = executeMatching(9891)
#candidates = executeMatching(10195)
#candidates = executeMatching(9891)
#candidates = executeMatching(9172)  
candidates = executeMatching(2454)
#candidates = executeMatching(9165)

for i in candidates:
    print(i.candidate_id)


#getQuizResults()