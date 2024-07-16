import ast
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import uuid
import pandas as pd
from ortools.sat.python import cp_model
from ortools.sat.python.cp_model import CpModel,CpSolver 
from pulp import *
from sqlalchemy import BigInteger, Column, String, create_engine, insert, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from free_version import free_running
from guided_version import guided_running

config = {
  'host': 'localhost',
  'user': 'root',
  'password':  'root@123',
  'database': 'scheduler',
  'port': 3306
}

# =======Connection to the database=======
try:
    encoded_password = quote_plus(config['password'])
    DATABASE_URI = f'mysql+pymysql://{config["user"]}:{encoded_password}@{config["host"]}:{config["port"]}/{config["database"]}'
    print(DATABASE_URI)
    
    # Create a SQLAlchemy engine
    engine = create_engine(DATABASE_URI)
except SQLAlchemyError as e:
    print(f"An error occurred while connecting to the database: {e}")

# Create a session
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class ForceTable(Base):
    __tablename__ = 'forceTable'
    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    userId = Column(String(100), nullable=False)
    actionId = Column(String(100), nullable=False, default=lambda: str(uuid.uuid4()))
    dateInitiatedStart = Column(BigInteger, nullable=False)
    dateInitiatedEnd = Column(BigInteger, nullable=False)


# =======Get data and convert to a DataFrame=======
tasks_data = """SELECT id, workspaceId, name, 'Task' AS type, from_unixtime(floor(startDate/1000)) AS startDate, from_unixtime(floor(endDate/1000)) AS endDate, boxingId, isLocked, isComplex, NULL AS members, createdBy FROM task WHERE startDate IS NOT NULL AND endDate IS NOT NULL 
UNION
SELECT id, workspaceId, name, 'Meeting' AS type, from_unixtime(floor(startTime/1000)) AS startDate, from_unixtime(floor(endTime/1000)) AS endDate, boxingId, isLocked, NULL AS isComplex, userIds AS members, createdBy FROM meeting WHERE startTime IS NOT NULL AND endTime IS NOT NULL;"""
tasks_data = pd.read_sql(tasks_data, engine)
tasks_data = tasks_data.sort_values('startDate')

mock_table = "SELECT id, workspaceId, name, type, from_unixtime(floor(startDate/1000)) AS startDate, from_unixtime(floor(endDate/1000)) AS endDate, userIds, boxingId, isContinuous, isLocked, isComplex, createdBy, isDeleted, isSimpleTask, isCompleted FROM mockData;"
mock_table = pd.read_sql(mock_table, engine)
mock_table = mock_table.sort_values('startDate')

limitation_table = """SELECT limitations.id as id, userId, limitationsType.name AS limitationsTypeName, subLimitationsType.name AS subLimitationsTypeName, activity.name AS activityName, suggestionAction.name AS suggestedActionName FROM limitations
                    LEFT JOIN limitationsType ON limitations.limitationsTypeId = limitationsType.id 
                    LEFT JOIN subLimitationsType ON limitations.subLimitationsTypeId = subLimitationsType.id 
                    LEFT JOIN activity ON limitations.activityId = activity.id 
                    LEFT JOIN suggestionAction ON limitations.suggestedActionId = suggestionAction.id;"""
limitation_table = pd.read_sql(limitation_table,engine)

user_table ="""SELECT * FROM user;"""
user_table = pd.read_sql(user_table,engine)

workspaceMember ="""SELECT workspaceMember.id AS id, workspaceId, userId, role.name AS role, role.position AS position FROM workspaceMember
                    LEFT JOIN user ON workspaceMember.userId = user.id
                    LEFT JOIN role ON workspaceMember.roleId = role.id;"""
workspaceMember = pd.read_sql(workspaceMember,engine)

role ="""SELECT * FROM role;"""
role = pd.read_sql(role,engine)

print(mock_table)
print()
print(limitation_table[['id', 'limitationsTypeName', 'subLimitationsTypeName', 'activityName', 'suggestedActionName']])

log_table = "SELECT * FROM scheduler.logs;"
log_table = pd.read_sql(log_table,engine)
training_logs = "SELECT * FROM scheduler.trainingLogs;"
training_logs = pd.read_sql(training_logs,engine)

holiday_data_query = "SELECT from_unixtime(floor(startDate/1000)) AS startDate, from_unixtime(floor(endDate/1000)) AS endDate FROM holiday;"
holiday_data = pd.read_sql(holiday_data_query, engine)

# =======Get all the dates task were scheduled on=======
date_dict = {}
def add_dates_to_dict(start, end, task_id):
    current_date = start
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        if date_str in date_dict:
            date_dict[date_str].append(task_id)
        else:
            date_dict[date_str] = [task_id]
        current_date += timedelta(days=1)

for _, row in mock_table.iterrows():
    add_dates_to_dict(row['startDate'], row['endDate'], row['id'])

# =======Get Datewise Schedule=======
tasks_by_date = pd.DataFrame(list(date_dict.items()), columns=['date', 'id']).sort_values(by='date').reset_index(drop=True)
tasks_by_date['date'] = pd.to_datetime(tasks_by_date['date'])
# print('\n=======Datewise Schedule:========\n', tasks_by_date)


# ======= Get date from user so that tasks on that date will be scheduled =======
new_Date = pd.to_datetime(input("\nEnter on what date you want to schedule task from: ")).date()
start_time = datetime.strptime(str(new_Date), '%Y-%m-%d') + timedelta(hours=8)
end_time = datetime.strptime(str(new_Date), '%Y-%m-%d') + timedelta(hours=19)

task_schedules_by_day = tasks_by_date[tasks_by_date['date'].dt.date == new_Date]
filtered_tasks = mock_table[(pd.to_datetime(mock_table['startDate']).dt.date <= new_Date) & (pd.to_datetime(mock_table['endDate']).dt.date >= new_Date)]



schedule = {}
# Iterate through each task to allocate time slots
for _, task in filtered_tasks.iterrows():
    task_start = max(start_time, pd.to_datetime(task['startDate']))
    task_end = min(end_time, pd.to_datetime(task['endDate']))
    
    if task['id'] not in schedule:
        schedule[task['id']] = {'id': task['id'], 'start_time': task_start, 'end_time': task_end}
    else:
        schedule[task['id']]['end_time'] = task_end

time_schedule = pd.DataFrame(schedule.values())
print(f'\n======= Schedule of {new_Date} ========\n', time_schedule)

timeframe = input(f"\nEnter timeframe you want to schedule from (e.g., 8:00 - 12:00): ")
x, y = timeframe.split(' - ')
schedule_Start_Time = pd.to_datetime(x).time()
schedule_End_Time = pd.to_datetime(y).time()

schedule_by_time = time_schedule[(pd.to_datetime(time_schedule['start_time']).dt.time <= schedule_End_Time)  & (schedule_Start_Time <= pd.to_datetime(time_schedule['end_time']).dt.time)]
schedule_by_time = pd.merge(schedule_by_time,mock_table[['id','workspaceId','name','type', 'userIds','boxingId','isContinuous','isLocked','isComplex','isSimpleTask', 'isDeleted', 'isCompleted','createdBy']],on='id', how='left')
print(f'\n======= Schedule of tasks between {schedule_Start_Time} - {schedule_End_Time} ========\n',schedule_by_time)


schedule_Start_Time = pd.Timestamp(f'{new_Date} {schedule_Start_Time}')
schedule_End_Time = pd.Timestamp(f'{new_Date} {schedule_End_Time}')

force_table = ForceTable(
    userId="47e5aaa3-d222-479d-a5f0-c5deab9db733",
    dateInitiatedStart=schedule_Start_Time.value // 10**6,
    dateInitiatedEnd=schedule_End_Time.value // 10**6          
)
session.add(force_table)
session.commit()

force_table_action_Id = force_table.actionId


for _,task in schedule_by_time.iterrows():
    schedule = schedule_by_time[schedule_by_time['id'] == task['id']]
    if 'limitations' not in schedule_by_time.columns:
            schedule_by_time['limitations'] = None

    if 'Not Over Pass Deadline' in limitation_table['limitationsTypeName'].values:
        schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'Not Over Pass Deadline' + ','

    if 'holiday or weekend time period' in limitation_table['limitationsTypeName'].values:
        schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'holiday or weekend time period' + ','


    if 'How long ago meeting was set' in limitation_table['limitationsTypeName'].values:
        task_start_date = pd.Timestamp(mock_table[mock_table['id'] == task['id']]['startDate'].values[0])
        gap = pd.to_timedelta(schedule_Start_Time - task_start_date)
        if gap > pd.Timedelta(weeks=3):
            schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'How long ago meeting was set' + ','

    if (schedule['boxingId'].values[0] != None) & ('Box Time' in limitation_table['limitationsTypeName'].values ):
            schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'Box Time' + ','

    if (schedule['isLocked'].values[0] == 1) & ('Locked Meeting/Tasks' in limitation_table['subLimitationsTypeName'].values):
            schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'Locked Meeting/Tasks' + ','

    if schedule['type'].values[0] == 'Task':
            if (schedule['isComplex'].values[0] == 1) & ('Complex Task' in limitation_table['activityName'].values):
                schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'Complex Task' + ','

    if 'External call with Client' in limitation_table['subLimitationsTypeName'].values:
        host = schedule['createdBy'].values[0]
        if user_table[user_table['id'] == host]['clientId'].values[0] != None:
            schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'External call with Client' + ','

    if 'Internal Meeting With Higher Hierarchy' in limitation_table['limitationsTypeName'].values:
        host = schedule['createdBy'].values[0]
        users_list = schedule['userIds'].values
        workspace = schedule['workspaceId'].values[0]
        user_positions = []
        if user_table[user_table['id'] == host]['clientId'].values[0] != None:
                print()
        elif (len(users_list) != 0) & (users_list[0] != None):
            for user_str in users_list:
                user_ids = user_str.split(',')
                for user in user_ids:
                    user_positions.append(workspaceMember[(workspaceMember['workspaceId'] == workspace) & (workspaceMember['userId'] == user)]['position'].values[0])
        if len(user_positions) != 0:
                for position in user_positions:
                    if workspaceMember[(workspaceMember['userId'] == host) & (workspaceMember['workspaceId'] == workspace)]['position'].values[0] > position:
                        schedule_by_time.loc[schedule_by_time['id'] == task['id'], 'limitations'] = schedule_by_time['limitations'].fillna('') + 'Internal Meeting With Higher Hierarchy' + ','


schedule_by_time['limitations'] = schedule_by_time['limitations'].str.rstrip(',')

print()
print(schedule_by_time)

# schedule_by_time = schedule_by_time[~schedule_by_time['limitations'].apply(lambda x: 'Box Time' in x or 'Complex Task' in x or 'Locked Meeting/Tasks' in x if isinstance(x, str) else False)]

print()
for i,row in schedule_by_time.iterrows():
    limitations = row['limitations']
    if ('Box Time' in limitations) & (limitation_table[limitation_table['subLimitationsTypeName'] == 'Deep Work']['suggestedActionName'].values[0] == 'No Action'):
        user_input = input(f"Box Time is set for task {row['id']}, It can't be rescheduled. Do you want to set another box_time?(yes/no): ")
        if 'yes' not in user_input.lower():
            schedule_by_time = schedule_by_time.drop([i])
    if ('Complex Task' in limitations) & (limitation_table[limitation_table['activityName'] == 'Complex Task']['suggestedActionName'].values[0] == 'No Action'):
        user_input = input(f"Task {row['id']} is complex task, It can't be rescheduled. Do you still want to reschedule?(yes/no): ")
        if 'yes' not in user_input.lower():
            schedule_by_time = schedule_by_time.drop([i])
    if 'Locked Meeting/Tasks' in limitations:
        print(f"Task {row['id']} is locked, It can't be rescheduled.")
        schedule_by_time = schedule_by_time.drop([i])
    if 'Internal Meeting With Higher Hierarchy' in limitations:
        print(f"Higher hierarchy will be present on that day, It can't be rescheduled.")
        schedule_by_time = schedule_by_time.drop([i])


# print()
print(schedule_by_time)


def tool_selection(task_list,schedule_by_time,task_duration,schedule_Start_Time,available_slots,available_slots_days):
    print()
    # ======= Ask user what method should be followed =======
    running_type = input("You want to go for free running version or guided version: ")
    if 'free' in running_type.lower():
        free_running(task_list,schedule_by_time,task_duration,schedule_Start_Time,schedule_End_Time,available_slots,available_slots_days)
    elif 'guided' in running_type.lower() or 'directed' in running_type.lower():
        guided_running(task_list,schedule_by_time,schedule_Start_Time,schedule_End_Time,available_slots)


# =======Availabilty of time slots based on their previous duration required=======
def find_next_available_slot(schedule, duration):
    available_slots = []

    for i in range(len(schedule) - 1):
        current_end = schedule.iloc[i]['endDate']
        next_start = schedule.iloc[i + 1]['startDate']
        if next_start - current_end >= duration:
            available_slots.append((current_end, next_start))
    
    return available_slots

# ======= Get avilable slots and dictonary for task and their previous durations =======
available_slots = []
task_duration = {}
if schedule_by_time.empty:
   print("\nThere is no task or meeting scheduled for that day for that time period!")
else:
    task_list = [row['id'] for _, row in schedule_by_time.iterrows()]
    # print("\n====Tasks/Meetings list on that day between that time period====\n", task_list)

    for task_id in task_list:
        duration = pd.to_timedelta(mock_table[mock_table['id'] == task_id]['endDate'].values[0] - mock_table[mock_table['id'] == task_id]['startDate'].values[0])
        task_duration[task_id] = int(duration.total_seconds())

        slots= find_next_available_slot(mock_table,duration = pd.Timedelta(minutes=30))

        if (mock_table[mock_table['id'] == task_id]['endDate'].values[0] - schedule_End_Time) >= pd.Timedelta(minutes=30):
            available_slots.append((schedule_End_Time, pd.Timestamp(mock_table[mock_table['id'] == task_id]['endDate'].values[0])))
        for slot in slots:
            if (slot[0].date() >= new_Date):
                if slot not in available_slots:
                    available_slots.append(slot)
    
    available_slots_days = [(int((end - pd.Timestamp(mock_table['startDate'].min())).total_seconds()), int((start - pd.Timestamp(mock_table['startDate'].min())).total_seconds()))
                            for start, end in available_slots]
def filter_slots_by_holidays(available_slots, holiday_data):
    filtered_slots = []
    
    for slot in available_slots:
        slot_start, slot_end = slot
        
        overlaps_holiday = False
        for _, holiday in holiday_data.iterrows():
            holiday_start = holiday['startDate']
            holiday_end = holiday['endDate']
            
            # Check if the slot overlaps with the holiday period
            if not (slot_end <= holiday_start or slot_start >= holiday_end):
                overlaps_holiday = True
                break
        
        if not overlaps_holiday:
            filtered_slots.append(slot)
    
    return filtered_slots

    # ======= Apply holiday filtering =======
available_slots = filter_slots_by_holidays(available_slots, holiday_data)
tool_selection(task_list,schedule_by_time,task_duration,schedule_Start_Time,available_slots,available_slots_days)

# print()
# print(task_duration)
# for slot in available_slots:
#     print(f"Available_slots: {slot[0]} to {slot[1]}")
# print()
# print(available_slots_days)