import boto3
import json
import time
import os
from langchain import PromptTemplate
import random

import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector

bedrock_client = boto3.client(service_name="bedrock-runtime")

# Define variables for database connection details
# If the database is deployed by your cloudformation,  you can get these values from the output value of your cloudformation stack
db_host = "<rds endpoint>"
db_password = "<rds password>"  # specified in your parameters file when deploying cloudformation template
db_user = "<rds username>"  # specified in your parameters file when deploying cloudformation template

# Establish the database connection using the variables
mydb = mysql.connector.connect(host=db_host, user=db_user, password=db_password)

# check all the databases already in your test database.
mycursor = mydb.cursor()

mycursor.execute("SHOW DATABASES")

for x in mycursor:
    print(x)

# Build database
mycursor.execute("DROP TABLE IF EXISTS LLM_DEMO.TEST_EMPLOYEE_LLM")
mycursor.execute("DROP DATABASE IF EXISTS LLM_DEMO")
mycursor.execute("CREATE DATABASE LLM_DEMO")

mycursor.execute(
    """
CREATE TABLE LLM_DEMO.TEST_EMPLOYEE_LLM -- Table name
(EMPID INT(10), -- employee id of the employee
NAME VARCHAR(20), -- name of the employee
SALARY INT(10), -- salary that the employee gets or makes
BONUS INT(10),-- bonus that the employee gets or makes
CITY VARCHAR(20), -- city where employees work from or belongs to
JOINING_DATE TIMESTAMP,-- date of joining for the employee
ACTIVE_EMPLOYEE INT(2), -- whether the employee is active(1) or in active(0)
DEPARTMENT VARCHAR(20), -- the deparment name where employee works or belongs to
TITLE VARCHAR(20) -- the title in office which employees has or holds
)
"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (1, 'Xyon McFluff', 50000, 10000, 'New York', '2020-01-01 10:00:00', 1, 'Engineering', 'Manager');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE) 
VALUES (2, 'Twinkle Luna', 60000, 5000, 'Chicago', '2018-05-15 11:30:00', 1, 'Sales', 'Executive');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (3, 'Zorfendorf', 45000, 2000, 'Miami', '2021-09-01 09:15:00', 1, 'Marketing', 'Associate');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)  
VALUES (4, 'Gloobinorg', 72000, 8000, 'Seattle', '2017-04-05 14:20:00', 1, 'IT', 'Manager');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (5, 'Bonkliwop', 65000, 6000, 'Denver', '2020-11-24 08:45:00', 1, 'Sales', 'Associate');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (6, 'Ploopdewoop', 55000, 4000, 'Philadelphia', '2019-03-11 10:25:00', 1, 'Marketing', 'Executive');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (7, 'Flooblelobber', 80000, 9000, 'San Francisco', '2016-08-20 12:35:00', 1, 'Engineering', 'Lead');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)  
VALUES (8, 'Blippitybloop', 57000, 3000, 'Boston', '2018-12-01 15:00:00', 1, 'Finance', 'Analyst');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (9, 'Snorkeldink', 74000, 7000, 'Atlanta', '2015-10-07 16:15:00', 1, 'IT', 'Lead');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (10, 'Wuggawugga', 69000, 5000, 'Austin', '2017-06-19 13:45:00', 1, 'Engineering', 'Manager'); """
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (11, 'Foofletoot', 62000, 4000, 'San Diego', '2019-02-24 17:30:00', 1, 'Sales', 'Associate');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (12, 'Bonkbonk', 82000, 8000, 'Silicon Valley', '2014-12-05 09:45:00', 1, 'Engineering', 'Director');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (13, 'Zippityzoom', 78000, 7500, 'New York', '2016-03-08 11:00:00', 1, 'IT', 'Manager');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE) 
VALUES (14, 'Splatchsplatch', 90000, 9500, 'Chicago', '2013-01-26 13:15:00', 1, 'Marketing', 'Director');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)  
VALUES (15, 'Wuggles', 85000, 8000, 'Seattle', '2018-07-22 15:30:00', 1, 'Finance', 'Manager');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (16, 'Boingboing', 70000, 6000, 'Miami', '2020-04-11 16:45:00', 1, 'Sales', 'Lead');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (17, 'Zipzoom', 62000, 5000, 'Denver', '2017-09-18 18:00:00', 1, 'Engineering', 'Associate');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)  
VALUES (18, 'Wooglewoogle', 58000, 3500, 'Philadelphia', '2019-12-24 08:20:00', 1, 'IT', 'Analyst');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE)
VALUES (19, 'Flipflopglop', 75000, 7200, 'Boston', '2022-02-14 10:35:00', 1, 'Marketing', 'Lead');"""
)

mycursor.execute(
    """INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS, CITY, JOINING_DATE, ACTIVE_EMPLOYEE, DEPARTMENT, TITLE) 
VALUES (20, 'Blipblop', 68000, 6500, 'San Francisco', '2021-08-29 11:50:00', 1, 'Finance', 'Executive');"""
)


mydb.commit()


# Verify our database connection works and we can retrieve records from our table
mycursor.execute("SELECT * FROM LLM_DEMO.TEST_EMPLOYEE_LLM")

myresult = mycursor.fetchall()

for x in myresult:
    print(x)


# Define a function to interact with a database using an SQL query.
# Arguments:
#   llm_generated_response: A string containing an SQL query to execute.
# Returns:
#   A formatted string containing the results of the executed SQL query.
def callDatabase(llm_generated_response):
    mycursor = mydb.cursor()

    mycursor.execute(llm_generated_response)

    myresult = mycursor.fetchall()

    output_string = ""
    for x in myresult:
        output_string = output_string + str(x) + "\n"
        print(x)

    return output_string


# Interact with a large language model (LLM) to generate text
# based on a prompt.
#
# Arguments:
#   prompt: The text prompt to provide to the LLM.
#   llm_type: The name of the LLM to use, either 'titan' or 'claude'.
#
# Returns:
#   The text generated by the LLM in response to the prompt.
#
# This function:
# 1. Prints the llm_type for debugging.
# 2. Formats the prompt into the JSON payload expected by each LLM API.
# 3. Specifies the parameters for text generation like max tokens, temp.
# 4. Calls the Bedrock client to invoke the LLM model API.
# 5. Parses the response to extract the generated text.
# 6. Returns the generated text string.


def interactWithLLM(prompt, type):
    if type == "titan":
        print("**THE LLM TYPE IS -->" + type)
        print("prompt---->" + prompt)
        # Test for invoke model begins
        parameters = {
            "maxTokenCount": 512,
            "stopSequences": [],
            "temperature": 0,
            "topP": 0.9,
        }
        body = json.dumps({"inputText": prompt, "textGenerationConfig": parameters})
        modelId = "amazon.titan-tg1-large"  # "amazon.titan-tg1-large"
        accept = "application/json"
        contentType = "application/json"
        response = bedrock_client.invoke_model(
            body=body, modelId=modelId, accept=accept, contentType=contentType
        )
        response_body = json.loads(response.get("body").read())

        response_text_titan = response_body.get("results")[0].get("outputText")

        return response_text_titan

    elif type == "claude":
        print("**THE LLM TYPE IS -->" + type)
        body = json.dumps(
            {
                "prompt": prompt,
                "max_tokens_to_sample": 2048,
                "temperature": 1,
                "top_k": 250,
                "top_p": 0.999,
                "stop_sequences": [],
            }
        )
        modelId = "anthropic.claude-v2"  # change this to use a different version from the model provider
        accept = "application/json"
        contentType = "application/json"
        print("prompt---->" + prompt)
        response = bedrock_client.invoke_model(
            body=body, modelId=modelId, accept=accept, contentType=contentType
        )
        response_body = json.loads(response.get("body").read())

        response_text_claude = response_body.get("completion")

        return response_text_claude


# Define a function to prepare the final generated text by combining a given prompt and database query results.
# Arguments:
#   prompt_final: A string representing a prompt template for text generation.
#   output_string: A string containing formatted database query results.
# Returns:
#   The final generated text based on the combined prompt and database query results.


def prepareFinalGenText(prompt_final, output_string):
    prompt_template_for_query_response = PromptTemplate.from_template(prompt_final)

    prompt_data_for_query_response = prompt_template_for_query_response.format(
        question=question_asked, answer=output_string
    )
    # print(prompt_data_for_query_response)
    final_response_text = interactWithLLM(prompt_data_for_query_response, "claude")
    return final_response_text


# prompt for the final generated text based on the combined prompt and database query result

prompt_final = """
Human: Based on  the question below

{question}

the answer was given below. 

{answer}

Provide answer in simple english statement and don't include table or schema names.
Assistant: 
"""

# prompt for in-context SQL generation based on NLP question

prompt = """
Human:  You are a mysql query expert whose output is a valid sql query. 

Only use the following tables:

It has the following schema:
<table_schema>
CREATE TABLE LLM_DEMO.TEST_EMPLOYEE_LLM -- Table name
(EMPID INT(10), -- employee id of the employee
NAME VARCHAR(20), -- name of the employee
SALARY INT(10), -- salary that the employee gets or makes
BONUS INT(10),-- bonus that the employee gets or makes
CITY VARCHAR(20), -- city where employees work from or belongs to
JOINING_DATE TIMESTAMP,-- date of joining for the employee
ACTIVE_EMPLOYEE INT(2), -- whether the employee is active(1) or in active(0)
DEPARTMENT VARCHAR(20), -- the deparment name where employee works or belongs to
TITLE VARCHAR(20) -- the title in office which employees has or holds
)
<table_schema>

The schema name is LLM_DEMO

And here is a sample insert statement or record for your reference : 

INSERT INTO LLM_DEMO.TEST_EMPLOYEE_LLM (EMPID, NAME, SALARY, BONUS,CITY,JOINING_DATE,ACTIVE_EMPLOYEE,DEPARTMENT,TITLE) VALUES (1, 'Stuart', 25000, 5000, 'Seattle','2023-01-21 00:00:01',1,'Applications','Sr. Developer');

Please construct a valid SQL statement to answer the following the question, return only the mysql query in between <sql></sql>.:

Question: {question}

Assistant: 
"""

# Execute few shot prompt
question_asked = (
    "What is the total count of employees who are active in each department?"
)
print(f"question_asked: {question_asked}")
prompt_template_for_query_generate = PromptTemplate.from_template(prompt)
prompt_data_for_query_generate = prompt_template_for_query_generate.format(
    question=question_asked
)


llm_generated_response = interactWithLLM(prompt_data_for_query_generate, "claude")
print(llm_generated_response)

llm_generated_response = llm_generated_response.replace("<sql>", "")
llm_generated_response = llm_generated_response.replace("</sql>", " ")


output_string = callDatabase(llm_generated_response)

prepareFinalGenText(prompt_final, output_string)

question_asked = "What is the average salary of employees in each department?"
print(f"question_asked: {question_asked}")
prompt_template_for_query_generate = PromptTemplate.from_template(prompt)
prompt_data_for_query_generate = prompt_template_for_query_generate.format(
    question=question_asked
)

llm_generated_response = interactWithLLM(prompt_data_for_query_generate, "claude")
print(llm_generated_response)

llm_generated_response = llm_generated_response.replace("<sql>", "")
llm_generated_response = llm_generated_response.replace("</sql>", " ")

output_string = callDatabase(llm_generated_response)

prepareFinalGenText(prompt_final, output_string)
