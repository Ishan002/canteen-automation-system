import uuid
import pandas as pd 
from pulp import *
from urllib.parse import quote_plus
from sqlalchemy import Column, String, create_engine, insert, DateTime, Boolean
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, Column, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


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

Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

#  Define the Logs class
class Log(Base):
    __tablename__ = 'logs'
    
    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    taskMeetingId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    originalStartDate = Column(DateTime, nullable=False)
    originalEndDate = Column(DateTime, nullable=False)
    toolSuggestion = Column(String(1000), nullable=True)
    userAction = Column(String(100), nullable=True)
    toolAction = Column(String(100), nullable=True)
    pendingAction = Column(Boolean, default=False)

class CurrentState(Base):
    __tablename__ = 'currentState'

    id = Column(String(100), primary_key=True, nullable=False)
    taskId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    startDate = Column(BigInteger, nullable=False)
    endDate = Column(BigInteger, nullable=False)
    createdBy = Column(String(100), nullable=False)
    actionId = Column(String(100), nullable=False)

class tool_Report(Base):
    __tablename__ = 'toolReport'

    id = Column(String(100), primary_key=True, nullable=False)
    taskId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    originalStartDate = Column(BigInteger, nullable=False)
    originalEndDate = Column(BigInteger, nullable=False)
    toolSuggestions = Column(String(300), nullable=False)
    actionId = Column(String(100), nullable=False)

class AfterState(Base):
    __tablename__ = 'afterState'

    id = Column(String(100), primary_key=True, nullable=False)
    taskId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    originalStartDate = Column(BigInteger, nullable=False)
    originalEndDate = Column(BigInteger, nullable=False)
    userAction = Column(String(300), nullable=False)
    toolAction = Column(String(100), nullable=False)
    actionId = Column(String(100), nullable=False)


class TrainingLog(Base):
    __tablename__ = 'trainingLogs'
    
    id = Column(String(100), primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    taskMeetingId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    originalStartDate = Column(DateTime, nullable=False)
    originalEndDate = Column(DateTime, nullable=False)
    toolSuggestion = Column(String(1000), nullable=True)
    userAction = Column(String(100), nullable=True)
    toolAction = Column(String(100), nullable=True)

# =======Get data and convert to a DataFrame=======
tasks_data = """SELECT id, workspaceId, name, 'Task' AS type, from_unixtime(floor(startDate/1000)) AS startDate, from_unixtime(floor(endDate/1000)) AS endDate, boxingId, isLocked, isComplex, NULL AS members, createdBy FROM task WHERE startDate IS NOT NULL AND endDate IS NOT NULL 
UNION
SELECT id, workspaceId, name, 'Meeting' AS type, from_unixtime(floor(startTime/1000)) AS startDate, from_unixtime(floor(endTime/1000)) AS endDate, boxingId, isLocked, NULL AS isComplex, userIds AS members, createdBy FROM meeting WHERE startTime IS NOT NULL AND endTime IS NOT NULL;"""
tasks_data = pd.read_sql(tasks_data, engine)
tasks_data = tasks_data.sort_values('startDate')

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


log_table = "SELECT * FROM scheduler.logs;"
log_table = pd.read_sql(log_table,engine)

mock_table = "SELECT id, workspaceId, name, type, from_unixtime(floor(startDate/1000)) AS startDate, from_unixtime(floor(endDate/1000)) AS endDate, userIds, boxingId, isContinuous, isLocked, isComplex, createdBy, isDeleted, isSimpleTask, isCompleted FROM mockData;"
mock_table = pd.read_sql(mock_table, engine)
mock_table = mock_table.sort_values('startDate')

def linear_problem(id, user_input, duration_seconds):
    linear_problem = LpProblem("Task_Scheduling", LpMinimize)

    start_time_var = LpVariable(f"Task_{id}", lowBound=user_input.timestamp())
    end_time_var = LpVariable(f"end_time_{id}", lowBound= user_input.timestamp())

    linear_problem += (end_time_var - start_time_var), f"Duration_of_reschedule_task"


    linear_problem += (end_time_var-start_time_var == duration_seconds), f"Duration_Constraint_{id}"
    linear_problem += (start_time_var == user_input.timestamp()), f"User_Start_Time_Constraint_{id}"

    linear_problem.solve()

    slot = f"{(pd.to_datetime(value(start_time_var), unit='s'), pd.to_datetime(value(end_time_var), unit='s'))}"
    tool_action = "Scheduled based on user's choice."

    print(f"Task {id} scheduled from {pd.to_datetime(value(start_time_var), unit='s')} to {pd.to_datetime(value(end_time_var), unit='s')}")

    return slot,tool_action



def guided_extended_block(schedule,task_id,available_slots):
    deadline = False
    if 'Not Over Pass Deadline' in schedule['limitations'].values[0]:
                deadline = True
                print()
                print(f"Task {task_id} should be completed before {pd.Timestamp(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0])}")

    suggestion = f"Slots available: {available_slots[:2]}"
    print(f"\nNext two available slots are: {available_slots[:2]}\n")
    user_input = pd.Timestamp(input(f"\nWhen do you want to schedule task with id {task_id} : "))

    duration = pd.to_timedelta(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0] - tasks_data[tasks_data['id'] == task_id]['startDate'].values[0])


    def calc_problem(duration):
        duration_seconds = int(duration.total_seconds())
        slot, tool_action = linear_problem(task_id, user_input, duration_seconds)
        return slot, tool_action


    if deadline:
        changed_duration = pd.to_timedelta(pd.Timestamp(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]) - user_input)
        confirmation = input(f"\nYou have {changed_duration} to complete the task, Are you sure you want to move the task? : ")

        if confirmation.lower() == 'yes' :
            slot,tool_action = calc_problem(changed_duration)
        else:
            slot = "No Action"
            tool_action = "No Action"
    else:
        slot,tool_action = calc_problem(duration)

    return suggestion, slot, tool_action

def guided_running(task_list,schedule_by_time,schedule_Start_Time,available_slots,force_table_action_Id):
    for task_id in task_list:
        suggestion = None
        tool_action = None
        user_action = None
        pending_Action = False
        schedule = schedule_by_time[schedule_by_time['id'] == task_id]


        if schedule['type'].values[0] == 'Task':
        
            task_start_date = pd.Timestamp(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0])
            gap = pd.to_timedelta(schedule_Start_Time - task_start_date)
            if (gap > pd.Timedelta(weeks=3)) & ('How long ago meeting was set' in schedule['limitations'].values[0]):
                suggestion = f"Task should not be moved as it was scheduled long ago on {task_start_date}"
                confirmation = input(f"\nTask {task_id} should not be moved as it was scheduled long ago on {task_start_date}, do you still want to move this task?: ")
                if confirmation.lower() == 'yes':
                    suggestion, user_action, tool_action = guided_extended_block(schedule,task_id,available_slots)
                else:
                    user_action = "No Action"
                    tool_action = "No Action"
            else:
                suggestion, user_action, tool_action = guided_extended_block(schedule,task_id,available_slots)

        elif schedule['type'].values[0] == 'Meeting':
            print()
            print(tasks_data[tasks_data['id'] == task_id][['members','createdBy']])
            host = schedule['createdBy'].values[0]
            if "External call with Client" in schedule['limitations'].values[0]:
                if user_table[user_table['id'] == host]['clientId'].values[0] != None:
                    print("\nThis is a client meeting!")
                    if limitation_table[limitation_table['subLimitationsTypeName'] == "External call with Client"]['suggestedActionName'].values[0] == 'Send email to client suggesting next two avilable slots':
                        suggestion = f"Slots available: {available_slots[:2]}"
                        tool_action = f"Suggested slot to client for selection"
                        print(f"\nClient can schedule meeting {task_id} from these next available slots: {available_slots[:2]}\n")
                        pending_Action = True
                

        

        new_log = Log(
            taskMeetingId=task_id,
            name=schedule['name'].values[0],
            type=schedule['type'].values[0],
            originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]),
            originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]),
            toolSuggestion=suggestion,
            userAction=user_action,
            toolAction=tool_action,
            pendingAction = pending_Action
        )

    
        mock_data = mock_table[mock_table['id'] == task_id].iloc[0]
        current_state = CurrentState(
            id=str(uuid.uuid4()),  # Generate a new UUID for the id
            taskId=mock_data['workspaceId'],
            name=mock_data['name'],
            type=mock_data['type'],
            startDate=int(pd.Timestamp(mock_data['startDate']).timestamp())*1000,
            endDate=int(pd.Timestamp(mock_data['endDate']).timestamp())*1000,
            createdBy=mock_data['createdBy'],   
            actionId=force_table_action_Id
        )

        
        tool_report = tool_Report(
            id=str(uuid.uuid4()),  # Generate a new UUID for the id
            taskId=new_log.taskMeetingId,
            name=new_log.name,
            type=new_log.type,
            originalStartDate = int(pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]).timestamp()) * 1000,
            originalEndDate = int(pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]).timestamp()) * 1000,
            toolSuggestions=new_log.toolSuggestion,
            actionId=force_table_action_Id

)

        
        after_state = AfterState(
            id=str(uuid.uuid4()),  # Generate a new UUID for the id
            taskId=new_log.taskMeetingId,
            name=new_log.name,
            type=new_log.type,
            originalStartDate = int(pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]).timestamp()) * 1000,
            originalEndDate = int(pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]).timestamp()) * 1000,
            userAction=new_log.userAction,
            toolAction=new_log.toolAction,
            actionId=force_table_action_Id

        )
        
        training_logs = TrainingLog(
            taskMeetingId=task_id,
            name=schedule['name'].values[0],
            type=schedule['type'].values[0],
            originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]),
            originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]),
            toolSuggestion=suggestion,
            userAction=user_action,
            toolAction=tool_action,
        )

        session.add(new_log)
        session.add(current_state)
        session.add(after_state)
        session.add(tool_report)
        session.add(training_logs)
        session.commit()