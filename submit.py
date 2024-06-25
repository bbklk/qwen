import json


# 此类会被跑分服务器继承， 可以在类中自由添加自己的prompt构建逻辑, 除了parse_table 和 run_inference_llm 两个方法不可改动
# 注意千万不可修改类名和下面已提供的三个函数名称和参数， 这三个函数都会被跑分服务器调用
class submission():
    def __init__(self, table_meta_path):
        self.table_meta_path = table_meta_path

    # 此函数不可改动, 与跑分服务器端逻辑一致， 返回值 grouped_by_db_id 是数据库的元数据（包含所有验证测试集用到的数据库）
    # 请不要对此函数做任何改动
    def parse_table(self, table_meta_path):
        with open(table_meta_path, 'r') as db_meta:
            db_meta_info = json.load(db_meta)
        # 创建一个空字典来存储根据 db_id 分类的数据
        grouped_by_db_id = {}

        # 遍历列表中的每个字典
        for item in db_meta_info:
            # 获取当前字典的 db_id
            db_id = item['db_id']

            # 如果 db_id 已存在于字典中，将当前字典追加到对应的列表
            if db_id in grouped_by_db_id:
                grouped_by_db_id[db_id].append(item)
            # 如果 db_id 不在字典中，为这个 db_id 创建一个新列表
            else:
                grouped_by_db_id[db_id] = [item]
        return grouped_by_db_id

    def convert_json_to_ddl(self, json_data):
        # Load JSON data
        data = json.loads(json_data)

        ddl_statements = []

        # Generate DDL statements for each table
        for table_index, table_name in enumerate(data[0]['table_names_original']):
            columns = []

            # Iterate over the column names and their respective table index
            for column_info in data[0]['column_names_original']:
                column_table_index, column_name = column_info

                if column_table_index == table_index or column_table_index == -1:
                    # Determine the appropriate data type based on the column name
                    if 'ID' in column_name:
                        data_type = "INT"
                    elif 'name' in column_name:
                        data_type = "VARCHAR(255)"
                    elif 'state' in column_name:
                        data_type = "VARCHAR(255)"
                    elif 'age' in column_name:
                        data_type = "INT"
                    elif 'Budget' in column_name:
                        data_type = "DECIMAL(10,2)"
                    elif 'Num' in column_name:
                        data_type = "INT"
                    else:
                        data_type = "VARCHAR(255)"  # Default to string data type

                    # Create the column definition
                    column_definition = f"{column_name} {data_type}"
                    columns.append(column_definition)

            # Add primary key constraint if the current table has primary keys
            if table_index in data[0]['primary_keys']:
                columns.append("PRIMARY KEY")

            # Create the DDL statement for the table
            ddl_statement = f"CREATE TABLE {table_name} ({', '.join(columns)});"
            ddl_statements.append(ddl_statement)

        # Generate foreign key constraints
        # for foreign_key in data['foreign_keys']:
        #     child_table_index, parent_table_index = foreign_key
        #     child_table_name = data['table_names_original'][child_table_index]
        #     parent_table_name = data['table_names_original'][parent_table_index]
        #     ddl_statement = f"ALTER TABLE {child_table_name} ADD FOREIGN KEY (head_ID) REFERENCES {parent_table_name}(Department_ID);"
        #     ddl_statements.append(ddl_statement)

        return ddl_statements


    # ddl_statements = convert_json_to_ddl(json_data)
    #
    # # Print the generated DDL statements
    # for ddl_statement in ddl_statements:
    #     print(ddl_statement)

    # 此为选手主要改动的逻辑， 此函数会被跑分服务器调用，用来构造最终输出给模型的prompt， 并对模型回复进行打分。
    # 当前提供了一个最基础的prompt模版， 选手需要对其进行改进
    def construct_prompt(self, current_user_question):
        question_type = current_user_question['question_type']
        user_question = current_user_question['user_question']
        if question_type == 'text2sql':
            current_db_id = current_user_question['db_id']
            cur_db_info = self.parse_table(self.table_meta_path)[current_db_id]
            cur_db_str = str(cur_db_info).replace("'", "\"")
            cur_db_ddl = self.convert_json_to_ddl(cur_db_str)
            cur_db_info1 = [{
                "column_names_original": [
                    [
                        -1,
                        "*"
                    ],
                    [
                        0,
                        "Mission_ID"
                    ],
                    [
                        0,
                        "Ship_ID"
                    ],
                    [
                        0,
                        "Code"
                    ],
                    [
                        0,
                        "Launched_Year"
                    ],
                    [
                        0,
                        "Location"
                    ],
                    [
                        0,
                        "Speed_knots"
                    ],
                    [
                        0,
                        "Fate"
                    ],
                    [
                        1,
                        "Ship_ID"
                    ],
                    [
                        1,
                        "Name"
                    ],
                    [
                        1,
                        "Type"
                    ],
                    [
                        1,
                        "Nationality"
                    ],
                    [
                        1,
                        "Tonnage"
                    ]
                ],
                "db_id": "ship_mission",
                "foreign_keys": [
                    [
                        2,
                        8
                    ]
                ],
                "primary_keys": [
                    1,
                    8
                ],
                "table_names_original": [
                    "mission",
                    "ship"
                ]
            }]
            cur_db_info2 = [{
                "column_names_original": [
                    [
                        -1,
                        "*"
                    ],
                    [
                        0,
                        "Department_ID"
                    ],
                    [
                        0,
                        "Name"
                    ],
                    [
                        0,
                        "Creation"
                    ],
                    [
                        0,
                        "Ranking"
                    ],
                    [
                        0,
                        "Budget_in_Billions"
                    ],
                    [
                        0,
                        "Num_Employees"
                    ],
                    [
                        1,
                        "head_ID"
                    ],
                    [
                        1,
                        "name"
                    ],
                    [
                        1,
                        "born_state"
                    ],
                    [
                        1,
                        "age"
                    ],
                    [
                        2,
                        "department_ID"
                    ],
                    [
                        2,
                        "head_ID"
                    ],
                    [
                        2,
                        "temporary_acting"
                    ]
                ],
                "db_id": "department_management",
                "foreign_keys": [
                    [
                        12,
                        7
                    ],
                    [
                        11,
                        1
                    ]
                ],
                "primary_keys": [
                    1,
                    7,
                    11
                ],
                "table_names_original": [
                    "department",
                    "head",
                    "management"
                ]
            }]
            cur_db_info3 = [{
                "column_names_original": [
                    [
                        -1,
                        "*"
                    ],
                    [
                        0,
                        "CITY_NAME"
                    ],
                    [
                        0,
                        "COUNTY"
                    ],
                    [
                        0,
                        "REGION"
                    ],
                    [
                        1,
                        "ID"
                    ],
                    [
                        1,
                        "NAME"
                    ],
                    [
                        1,
                        "FOOD_TYPE"
                    ],
                    [
                        1,
                        "CITY_NAME"
                    ],
                    [
                        1,
                        "RATING"
                    ],
                    [
                        2,
                        "RESTAURANT_ID"
                    ],
                    [
                        2,
                        "HOUSE_NUMBER"
                    ],
                    [
                        2,
                        "STREET_NAME"
                    ],
                    [
                        2,
                        "CITY_NAME"
                    ]
                ],
                "db_id": "restaurants",
                "foreign_keys": [
                    [
                        7,
                        1
                    ],
                    [
                        12,
                        1
                    ]
                ],
                "primary_keys": [
                    1,
                    4,
                    9
                ],
                "table_names_original": [
                    "GEOGRAPHIC",
                    "RESTAURANT",
                    "LOCATION"
                ]
            }]
            cur_db_info4 = [{
                "column_names_original": [
                    [
                        -1,
                        "*"
                    ],
                    [
                        0,
                        "venueId"
                    ],
                    [
                        0,
                        "venueName"
                    ],
                    [
                        1,
                        "authorId"
                    ],
                    [
                        1,
                        "authorName"
                    ],
                    [
                        2,
                        "datasetId"
                    ],
                    [
                        2,
                        "datasetName"
                    ],
                    [
                        3,
                        "journalId"
                    ],
                    [
                        3,
                        "journalName"
                    ],
                    [
                        4,
                        "keyphraseId"
                    ],
                    [
                        4,
                        "keyphraseName"
                    ],
                    [
                        5,
                        "paperId"
                    ],
                    [
                        5,
                        "title"
                    ],
                    [
                        5,
                        "venueId"
                    ],
                    [
                        5,
                        "year"
                    ],
                    [
                        5,
                        "numCiting"
                    ],
                    [
                        5,
                        "numCitedBy"
                    ],
                    [
                        5,
                        "journalId"
                    ],
                    [
                        6,
                        "citingPaperId"
                    ],
                    [
                        6,
                        "citedPaperId"
                    ],
                    [
                        7,
                        "paperId"
                    ],
                    [
                        7,
                        "datasetId"
                    ],
                    [
                        8,
                        "paperId"
                    ],
                    [
                        8,
                        "keyphraseId"
                    ],
                    [
                        9,
                        "paperId"
                    ],
                    [
                        9,
                        "authorId"
                    ]
                ],
                "db_id": "scholar",
                "foreign_keys": [
                    [
                        13,
                        1
                    ],
                    [
                        17,
                        7
                    ],
                    [
                        18,
                        11
                    ],
                    [
                        19,
                        11
                    ],
                    [
                        23,
                        9
                    ],
                    [
                        22,
                        11
                    ],
                    [
                        25,
                        3
                    ],
                    [
                        24,
                        11
                    ]
                ],
                "primary_keys": [
                    1,
                    3,
                    5,
                    7,
                    9,
                    11,
                    18,
                    21,
                    23,
                    24
                ],
                "table_names_original": [
                    "venue",
                    "author",
                    "dataset",
                    "journal",
                    "keyphrase",
                    "paper",
                    "cite",
                    "paperDataset",
                    "paperKeyphrase",
                    "writes"
                ]
            }]
            cur_db_str1 = str(cur_db_info1).replace("'", "\"")
            cur_db_ddl1 = self.convert_json_to_ddl(cur_db_str1)
            cur_db_str2 = str(cur_db_info2).replace("'", "\"")
            cur_db_ddl2 = self.convert_json_to_ddl(cur_db_str2)
            cur_db_str3 = str(cur_db_info3).replace("'", "\"")
            cur_db_ddl3 = self.convert_json_to_ddl(cur_db_str3)
            cur_db_str4 = str(cur_db_info4).replace("'", "\"")
            cur_db_ddl4 = self.convert_json_to_ddl(cur_db_str4)
            system_prompt = f"This is a text to sql task. You are a database expert, output can only be SQL queries that can be correctly operated. " \
                            f"DO NOT output ANY thought process, although you can take a long time of thinking and use chain of thought and solve the task step by step." \
                            f"Understand the meaning of user's order." \
                # f"The ((shorter)) output, the better. I want [short and correct] output. The output is in just one line."
            user_prompt = f"Your role: An experienced SQL database programmer, your output can only be SQL statements, if the SQL statement is wrong, you will be killed" \
                          f"Here are some reference examples:" \
                          f"    Database format is: {cur_db_ddl1}, Order: 既有吨位大于6000的船舶又有吨位小于4000的船舶的船舶类型是什么？" \
                          f"    SQL query: SELECT TYPE FROM ship WHERE Tonnage  >  6000 INTERSECT SELECT TYPE FROM ship WHERE Tonnage  <  4000 " \
                          f"    Database format is: {cur_db_ddl2}, Order: 排名在10到15之间的部门的平均雇员人数是多少？" \
                          f"    SQL query: SELECT AVG(num_employees) FROM department WHERE ranking BETWEEN 10 AND 15;" \
                          f"    Database format is: {cur_db_ddl3}, Order: give me a good french restaurant in the yosemite and mono lake area ?" \
                          f"    SQL query: SELECT t3.house_number  ,  t1.name FROM restaurant AS t1 JOIN geographic AS t2 ON t1.city_name  =  t2.city_name JOIN LOCATION AS t3 ON t1.id  =  t3.restaurant_id WHERE t2.region  =  \"yosemite and mono lake area\" AND t1.food_type  =  \"french\" AND t1.rating  >  2.5;" \
                          f"    Database format is: {cur_db_ddl4}, Order: Eric C. Kerrigan 's Liquid Automatica paper" \
                          f"    SQL query: SELECT DISTINCT t2.paperid FROM paperkeyphrase AS t5 JOIN keyphrase AS t3 ON t5.keyphraseid  =  t3.keyphraseid JOIN writes AS t4 ON t4.paperid  =  t5.paperid JOIN paper AS t2 ON t4.paperid  =  t2.paperid JOIN author AS t1 ON t4.authorid  =  t1.authorid JOIN venue AS t6 ON t6.venueid  =  t2.venueid WHERE t1.authorname  =  \"Eric C. Kerrigan\" AND t3.keyphrasename  =  \"Liquid\" AND t6.venuename  =  \"Automatica\";" \
                          f"Database format is: {cur_db_ddl}. Order: {user_question}.  SQL query: "

            temp_messages1 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages1)
            llm_outputs1 = llm_response
            system_prompt = "Your answer is JUST SQL query without '```sql'. You are a database expert, output can only be SQL query, not in the markdown format" \
                            "The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short and correct] output. " \
                            "The output is only SQL query without explanation"

            user_prompt = f"Database format is: {cur_db_ddl}. The order is {user_question}. My thought process and the SQL query is: {llm_outputs1} Based on my explanation, please judge if my answer is correct. And if you think my answer is wrong, make it correct. No need to explain. Your output should be SQL query, our updated answer is: "

            temp_messages2 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages2)
            llm_outputs2 = llm_response
            user_prompt = f"Given {llm_outputs2}. I want only SQL query without any other sentences or explanations especially do not output '```sql', our updated answer is: "

        elif question_type == 'multiple_choice':
            options = "A." + current_user_question['optionA'] + " B." + current_user_question['optionB'] + " C." + \
                      current_user_question['optionC'] + " D." + current_user_question['optionD']
            system_prompt = "This is a multi choice question. You are a database expert" \
                            "Solve the question step by step." \
                            "You can have long time to think about each choice before you make the correct answer. Please show the reason of your answer and thought process."
            user_prompt = f"Your role: An experienced SQL database programmer, if the answer is wrong, you will be killed" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Capable of outputting the only one correct answer from four options. " \
                          f"    3. Use the process of elimination to eliminate wrong answers one by one." \
                          f"Your task: The user will provide a multiple-choice question and the options. You need to provide the correct option. " \
                          f"Take your time to eliminate wrong answers one by one. Before you make decision, think if other choices can be correct, and select the most reasonable choice. Run the SQL statement in each choice, and see if the result is correct." \
                          f"If you select B, think about if C is correct, vise versa. If you select A, think about if D is correct, vice versa. If you select C, think about if A is correct, vise versa. If you select D, think about if B is correct, vise versa. And same for other choices." \
                          f"Here is the question {user_question} and the options are {options}. My choice and reason is:"

            temp_messages1 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages1)
            llm_outputs1 = llm_response

            system_prompt = "Your answer is JUST one letter. You are a database expert, output can only be one of the letters A, B, C, or D " \
                            "The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short and correct] output. " \
                            "The output is only one letter without period"

            user_prompt = f"I have a question {user_question} and the options are {options}. My thought process and the answer is: {llm_outputs1} Based on my explanation, please judge if my answer is correct. And if you think my answer is wrong, make it correct. No need to explain. Your output should be one letter, our updated answer is: "

            temp_messages2 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages2)
            llm_outputs2 = llm_response
            user_prompt = f"Given {llm_outputs2}. I want one letter answer without any other sentences or period, our updated answer is: "
            

        elif question_type == 'true_false_question':
            system_prompt = "This is a true or false question. You are a database expert. " \
                            "Solve the question step by step." \
                            "The ((correct)) output, the better. I want [correct] output. " \
                            "You can have long time to think about each choice before you make the correct answer.  Please show the reason of your answer and thought process."
            user_prompt = f"Your role: An experienced SQL database programmer, if the answer is wrong, you will be killed" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Can only output True or False as results. " \
                          f"    3. Use the process of elimination to eliminate wrong answer. " \
                          f"Take your time to eliminate wrong answers one by one. Before you make any choice, please think if the opposite can be correct, and select the most reasonable choice" \
                          f"Here are some reference examples:" \
                          f"    Question 1: In SQL, SELECT * is used to select all columns' data from a table." \
                          f"    Your reply: True" \
                          f"    Question 2: In SQL, the UPDATE statement can be used to delete records." \
                          f"    Your reply: False" \
                          f"Here is the question {user_question}. I think the answer is: "

            temp_messages1 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages1)
            llm_outputs1 = llm_response

            system_prompt = "Your answer is JUST one word without period. You are a database expert, output can only be one word 'True' or 'False' " \
                            "The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short and correct] output. " \
                            "The output is only one word without period without explanation."

            user_prompt = f"I have a question {user_question}. My thought process and the answer is: {llm_outputs1} Based on my explanation, please judge if my answer is correct. And if you think my answer is wrong, make it correct. No need to explain. Your output should be one word (True or False), our updated answer is: "

            temp_messages2 = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            llm_response = self.run_inference_llm(temp_messages2)
            llm_outputs2 = llm_response
            user_prompt = f"Given {llm_outputs2}. I want one word answer without period, our updated answer is: "

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        return messages

    # 此方法会被跑分服务器调用， messages 选手的 construct_prompt() 返回的结果
    # 请不要对此函数做任何改动
    def run_inference_llm(self, messages):
        pass