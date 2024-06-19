import json

# 此类会被跑分服务器继承， 可以在类中自由添加自己的prompt构建逻辑, 除了parse_table 和 run_inference_llm 两个方法不可改动
# 注意千万不可修改类名和下面已提供的三个函数名称和参数， 这三个函数都会被跑分服务器调用
class submission():
    def __init__(self, table_meta_path):
        self.table_meta_path = table_meta_path

    # 此函数不可改动, 与跑分服务器端逻辑一致， 返回值 grouped_by_db_id 是数据库的元数据（包含所有验证测试集用到的数据库）
    # 请不要对此函数做任何改动
    def parse_table(self, table_meta_path):
        with open(table_meta_path,'r') as db_meta:
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
            system_prompt = f"You are a database expert, output can only be SQL statements. The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short] output"
            user_prompt = f"Your role: An experienced SQL database programmer, your output can only be SQL statements" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge" \
                          f"    2. Well-versed in SQL statements and capable of ensuring the generated statements can run correctly" \
                          f"    3. Able to convert natural language into SQL statements that can run correctly" \
                          f"Your task: There is a database with the specific format: {cur_db_info}. The question is: {user_question}. Based on the provided information, generate SQL statements that can run correctly" \
                          f"Here are some reference examples:" \
                          f"    Question 1: I have a database named Student. It contains several tables. I want to query how many courses are offered by the school per semester and per year" \
                          f"    Your reply: SELECT count(*) , semester, YEAR FROM SECTION GROUP BY semester, YEAR; " \
                          f"    Question 2: What is the ship type that has both a tonnage greater than 6000 and a tonnage less than 4000?" \
                          f"    Your reply: SELECT TYPE FROM ship WHERE Tonnage > 6000 INTERSECT SELECT TYPE FROM ship WHERE Tonnage < 4000;" \
                          f"    Question 3: What is the average number of employees in departments ranked between 10 and 15?" \
                          f"    Your reply: SELECT AVG(num_employees) FROM department WHERE ranking BETWEEN 10 AND 15;" \
                          f"The shorter output, the better"

        elif question_type == 'multiple_choice':
            options = "A." + current_user_question['optionA'] + "B." + current_user_question['optionB']+ "C." + current_user_question['optionC']+ "D." + current_user_question['optionD']
            system_prompt = "You are a database expert, output can only be one of the letters A, B, C, or D. The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short] output"
            user_prompt = f"Your role: An experienced SQL database programmer" \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Capable of outputting the only one correct answer from four options. " \
                          f"Your task: The user will provide a multiple-choice question with the question as {user_question} and the options as {options}. You need to provide the correct option. " \
                          f"Here are some reference examples:" \
                          f"    Question 1: In SQL, the operator equivalent to 'NOT IN' is. " \
                          f"    Options: A. <>ALL B. <>SOME C. =SOME D. =ALL" \
                          f"    Your reply: A" \
                          f"    Question 2: There is a student table Student(Sno char(8), Sname char(10), Ssex char(2), Sage integer, Dno char(2), Sclass char(6)). To retrieve the 'age and name of all students with an age less than or equal to 18' from the student table, the correct SQL statement is." \
                          f"    Options: A. Select Sage, Sname From Student; B. Select * From Student Where Sage <= 18; C. Select Sage, Sname From Student Where Sage <= 18; D. Select Sname From Student Where Sage <= 18; " \
                          f"    Your reply: C" \
                          f"The shorter output, the better"
        elif question_type == 'true_false_question':
            system_prompt = "You are a database expert, output can only be True or False. The ((shorter)) output, the better. Shorter the output, make the output as short as possible. I want [short] output"
            user_prompt = f"Your role: An experienced SQL database programmer. " \
                          f"Your skills: " \
                          f"    1. Proficient in all aspects of SQL knowledge. " \
                          f"    2. Can only output True or False as results. " \
                          f"Your task: The user will provide a true/false question with the question as {user_question}. Please determine the truthfulness of the question (True/False). " \
                          f"Here are some reference examples:" \
                          f"    Question 1: In SQL, SELECT * is used to select all columns' data from a table." \
                          f"    Your reply: True" \
                          f"    Question 2: In SQL, the UPDATE statement can be used to delete records." \
                          f"    Your reply: False" \
                          f"The shorter output, the better"

        messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
        ]
        return messages

    # 此方法会被跑分服务器调用， messages 选手的 construct_prompt() 返回的结果
    # 请不要对此函数做任何改动
    def run_inference_llm(self, messages):
        pass