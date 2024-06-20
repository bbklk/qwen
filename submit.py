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

    # 此为选手主要改动的逻辑， 此函数会被跑分服务器调用，用来构造最终输出给模型的prompt， 并对模型回复进行打分。
    # 当前提供了一个最基础的prompt模版， 选手需要对其进行改进
    def construct_prompt(self, current_user_question):
        question_type = current_user_question['question_type']
        user_question = current_user_question['user_question']
        if question_type == 'text2sql':
            current_db_id = current_user_question['db_id']
            cur_db_info = self.parse_table(self.table_meta_path)[current_db_id]
            cur_db_info1 = self.parse_table('../external_data/sample_tables.json')['ship_mission']
            cur_db_info2 = self.parse_table('../external_data/sample_tables.json')['department_management']
            cur_db_info3 = self.parse_table('../external_data/sample_tables.json')['restaurants']
            cur_db_info4 = self.parse_table('../external_data/sample_tables.json')['scholar']
            system_prompt = f"This is a text to sql task. You are a database expert, output can only be SQL queries that can be correctly operated. " \
                            f"DO NOT output ANY thought process, although you can take a long time of thinking and use chain of thought and solve the task step by step." \
                            f"Understand the meaning of user's order." \
                # f"The ((shorter)) output, the better. I want [short and correct] output. The output is in just one line."
            user_prompt = f"Your role: An experienced SQL database programmer, your output can only be SQL statements, if the SQL statement is wrong, you will be killed" \
                          f"Here are some reference examples:" \
                          f"    Database format is: {cur_db_info1}, Order: 既有吨位大于6000的船舶又有吨位小于4000的船舶的船舶类型是什么？" \
                          f"    SQL query: SELECT TYPE FROM ship WHERE Tonnage  >  6000 INTERSECT SELECT TYPE FROM ship WHERE Tonnage  <  4000 " \
                          f"    Database format is: {cur_db_info2}, Order: 排名在10到15之间的部门的平均雇员人数是多少？" \
                          f"    SQL query: SELECT AVG(num_employees) FROM department WHERE ranking BETWEEN 10 AND 15;" \
                          f"    Database format is: {cur_db_info3}, Order: give me a good french restaurant in the yosemite and mono lake area ?" \
                          f"    SQL query: SELECT t3.house_number  ,  t1.name FROM restaurant AS t1 JOIN geographic AS t2 ON t1.city_name  =  t2.city_name JOIN LOCATION AS t3 ON t1.id  =  t3.restaurant_id WHERE t2.region  =  \"yosemite and mono lake area\" AND t1.food_type  =  \"french\" AND t1.rating  >  2.5;" \
                          f"    Database format is: {cur_db_info4}, Order: Eric C. Kerrigan 's Liquid Automatica paper" \
                          f"    SQL query: SELECT DISTINCT t2.paperid FROM paperkeyphrase AS t5 JOIN keyphrase AS t3 ON t5.keyphraseid  =  t3.keyphraseid JOIN writes AS t4 ON t4.paperid  =  t5.paperid JOIN paper AS t2 ON t4.paperid  =  t2.paperid JOIN author AS t1 ON t4.authorid  =  t1.authorid JOIN venue AS t6 ON t6.venueid  =  t2.venueid WHERE t1.authorname  =  \"Eric C. Kerrigan\" AND t3.keyphrasename  =  \"Liquid\" AND t6.venuename  =  \"Automatica\";" \
                          f"Database format is: {cur_db_info}. Order: {user_question}.  SQL query: "

            # user_prompt = f"Your role: An experienced SQL database programmer, your output can only be SQL statements, if the SQL statement is wrong, you will be killed" \
            #               f"Your skills: " \
            #               f"    1. Proficient in all aspects of SQL knowledge" \
            #               f"    2. Well-versed in SQL queries and capable of ensuring the generated queries can run correctly" \
            #               f"    3. Able to convert order into SQL queries that can run correctly" \
            #               f"Your task: Based on the provided information, generate SQL queries that can run correctly" \
            #               f"Hide all of thought process. only output SQL queries, make sure your sql statement can fulfill the requirement" \
            #               f"Here are some reference examples:" \
            #               f"    Order 1: I have a database named Student. It contains several tables. I want to query how many courses are offered by the school per semester and per year" \
            #               f"    SQL query: SELECT count(*) , semester, YEAR FROM SECTION GROUP BY semester, YEAR; " \
            #               f"    Order 2: What is the ship type that has both a tonnage greater than 6000 and a tonnage less than 4000?" \
            #               f"    SQL query: SELECT TYPE FROM ship WHERE Tonnage > 6000 INTERSECT SELECT TYPE FROM ship WHERE Tonnage < 4000;" \
            #               f"    Order 3: What is the average number of employees in departments ranked between 10 and 15?" \
            #               f"    SQL query: SELECT AVG(num_employees) FROM department WHERE ranking BETWEEN 10 AND 15;" \
            #               f"Do not dare your output to be not in one line!!!" \
            #               f"There is a database with the specific format: {cur_db_info}. The order is: {user_question}. SQL query: "

        elif question_type == 'multiple_choice':
            options = "A." + current_user_question['optionA'] + "B." + current_user_question['optionB'] + "C." + \
                      current_user_question['optionC'] + "D." + current_user_question['optionD']
            system_prompt = "Your answer is JUST one letter. This is a multi choice question. You are a database expert, output can only be one of the letters A, B, C, or D. " \
                            "Solve the question step by step." \
                            "The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short and correct] output. " \
                            "You can have long time to think about each choice before you make the correct answer, and solve the question step by step."
            user_prompt = f"Your role: An experienced SQL database programmer, if the answer is wrong, you will be killed" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Capable of outputting the only one correct answer from four options. " \
                          f"    3. Use the process of elimination to eliminate wrong answers one by one." \
                          f"Your task: The user will provide a multiple-choice question and the options. You need to provide the correct option. " \
                          f"Take your time to eliminate wrong answers one by one. Before you make decision, think if other choices can be correct, and select the most reasonable choice. Run the SQL statement in each choice, and see if the result is correct." \
                          f"If you select B, think about if C is correct, vise versa. If you select A, think about if D is correct, vice versa. If you select C, think about if A is correct, vise versa. If you select D, think about if B is correct, vise versa. And same for other choices." \
                          f"DO NOT output ANY thought process. Do not dare your output to be longer than one letter!!!" \
                          f"Here is the question {user_question} and the options {options}. Your choice is:"
            # f"Here are some reference examples:" \
            # f"    Question 1: In SQL, the operator equivalent to 'NOT IN' is. " \
            # f"    Options: A. <>ALL B. <>SOME C. =SOME D. =ALL" \
            # f"    Your reply: A" \
            # f"    Question 2: There is a student table Student(Sno char(8), Sname char(10), Ssex char(2), Sage integer, Dno char(2), Sclass char(6)). To retrieve the 'age and name of all students with an age less than or equal to 18' from the student table, the correct SQL statement is." \
            # f"    Options: A. Select Sage, Sname From Student; B. Select * From Student Where Sage <= 18; C. Select Sage, Sname From Student Where Sage <= 18; D. Select Sname From Student Where Sage <= 18; " \
            # f"    Your reply: C"

        elif question_type == 'true_false_question':

            system_prompt = "This is a true or false question. You are a database expert, output can only be True or False. " \
                            "The ((correct)) output, the better. Shorter the output, make the output as short as possible. I want [correct] output. " \
                            "You can have long time to think about each choice before you make the correct answer"
            user_prompt = f"Your role: An experienced SQL database programmer, if the answer is wrong, you will be killed" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Can only output True or False as results. " \
                          f"    3. Use the process of elimination to eliminate wrong answer. " \
                          f"Take your time to eliminate wrong answers one by one. Before you make any choice, please think if the opposite can be correct, and select the most reasonable choice" \
                          f"Here is the question {user_question}. Please determine the truthfulness of the question (True/False). " \
                          f"Here are some reference examples:" \
                          f"    Question 1: In SQL, SELECT * is used to select all columns' data from a table." \
                          f"    Your reply: True" \
                          f"    Question 2: In SQL, the UPDATE statement can be used to delete records." \
                          f"    Your reply: False"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        return messages

    # 此方法会被跑分服务器调用， messages 选手的 construct_prompt() 返回的结果
    # 请不要对此函数做任何改动
    def run_inference_llm(self, messages):
        pass