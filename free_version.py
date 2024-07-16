import uuid
import pandas as pd 
from ortools.sat.python import cp_model
from ortools.sat.python.cp_model import CpModel,CpSolver
from urllib.parse import quote_plus
from sqlalchemy import Boolean, Column, String, create_engine, insert, DateTime
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine, Column, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

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
    originalStartDate = Column(DateTime, nullable=False)
    originalEndDate = Column(DateTime, nullable=False)
    toolSuggestions = Column(String(300), nullable=False)
    actionId = Column(String(100), nullable=False)

class AfterState(Base):
    __tablename__ = 'afterState'

    id = Column(String(100), primary_key=True, nullable=False)
    taskId = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    originalStartDate = Column(DateTime, nullable=False)
    originalEndDate = Column(DateTime, nullable=False)
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

def free_running(task_list,schedule_by_time,task_duration, schedule_Start_Time,available_slots,available_slots_days,force_table_action_Id):

    new_task_list = []
    # print(81,task_list)
    for task_id in task_list:
        user_action = None
        suggestion = None
        tool_action = None
        pending_Action = False
        schedule = schedule_by_time[schedule_by_time['id'] == task_id]

        # if (schedule['boxingId'].values[0] != None) & ('Box Time' in limitation_table['limitationsTypeName'].values):
        #     print(f"\nThere is Box Time set for {task_id}! Task should not be moved.")
        #     suggestion = limitation_table[limitation_table['limitationsTypeName'] == 'Box Time']['suggestedActionName'].values[0]
        #     del task_duration[task_id]

        # elif (schedule['boxingId'].values[0] == None) & (schedule['isLocked'].values[0] == 1) & ('Locked Meeting/Tasks' in limitation_table['subLimitationsTypeName'].values):
        #     print(f"\n{task_id} is Locked!")
        #     suggestion = limitation_table[limitation_table['subLimitationsTypeName'] == 'Locked Meeting/Tasks']['suggestedActionName'].values[0]
        #     if suggestion == 'No Action':
        #         print(f"\n{task_id} can not be moved/rescheduled.")
        #         del task_duration[task_id]

        if schedule['type'].values[0] == 'Task':
            # if schedule['isComplex'].values[0] == 1:
            #     suggestion = limitation_table[limitation_table['activityName'] == 'Complex Task']['suggestedActionName'].values[0]
            #     if suggestion == 'No Action':
            #         print(f"\nTask {task_id} is Complex Task! It can not be moved/rescheduled.")
            #         del task_duration[task_id]
            
            task_start_date = pd.Timestamp(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0])
            gap = pd.to_timedelta(schedule_Start_Time - task_start_date)
            if (gap > pd.Timedelta(weeks=3)) & ('How long ago meeting was set' in schedule['limitations'].values[0]):
                suggestion = f"Task should not be moved as it was scheduled long ago on {task_start_date}"
                confirmation = input(f"\nTask {task_id} should not be moved as it was scheduled long ago on {task_start_date}, do you still want to move this task?: ")
                if confirmation.lower() != 'yes':
                    del task_duration[task_id]
                    user_action = "No Action"
                    tool_action = "No Action"
                    # new_task_list.append(task_id)

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
                        del task_duration[task_id]
                        # new_task_list.append(task_id)
        


        # if 'Internal Meeting With Higher Hierarchy' in schedule['limitations']:
        #     users_list = schedule['members'].values
        #     workspace = schedule['workspaceId'].values[0]
        #     user_positions = []
        #     host = schedule['createdBy'].values[0]
        #     if user_table[user_table['id'] == host]['clientId'].values[0] != None:
        #         print()
        #     elif (len(users_list) != 0) & (users_list[0] != None):
        #         for user_str in users_list:
        #             user_ids = user_str.split(',')
        #             for user in user_ids:
        #                 user_positions.append(workspaceMember[(workspaceMember['workspaceId'] == workspace) & (workspaceMember['userId'] == user)]['position'].values[0])
        #     elif len(user_positions) != 0:
        #         for position in user_positions:
        #             if workspaceMember[workspaceMember['userId'] == host]['position'].values[0] < position:
        #                 new_task_list.append(task_id)
        #             else:
        #                 suggestion = limitation_table[limitation_table['limitationsTypeName'] == 'Internal Meeting With Higher Hierarchy']['suggestedActionName'].values[0]
        #                 print(1234,f"\nThis meeting scheduled by someone that is in higher position. Next action will be taken by them.")
        #                 del task_duration[task_id]
        #                 pending_Action = True
        #                 break
        
        # else:
        #     new_task_list.append(task_id)

        if task_id not in task_duration.keys():
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
            session.add(training_logs)
            session.commit()
    print()
    # print(123,task_duration)

    # =======Initiaize model for Constraint Programming Approach=======
    model = CpModel()

    # Create variables for each task's start time
    task_starts = {}
    task_ends = {}
    for task in task_duration:
        task_starts[task] = model.NewIntVar(0, sum(days[0] for days in available_slots_days), f'start_{task}')
        task_ends[task] = model.NewIntVar(0, sum(days[0] for days in available_slots_days), f'end_{task}')

    # =======Set constraints=======
    for task, original_duration in task_duration.items():
        fits_in_slot = []
        for slot_end, slot_start in available_slots_days:
            slot_start_time = slot_start
            slot_end_time = slot_end
            duration = slot_end_time - slot_start_time
            # print(123, duration)
            fits_in_slot.append(
                model.NewBoolVar(f"slot_{task}_{slot_start_time}_{slot_end_time}")
            )
            # print(167,fits_in_slot)
            # model.Add(original_duration <= duration).OnlyEnforceIf(fits_in_slot[-1])
            model.Add(task_starts[task] >= slot_start_time).OnlyEnforceIf(fits_in_slot[-1]) # Start time should be on or after the start time of available slot
            model.Add(task_ends[task] <= slot_end_time).OnlyEnforceIf(fits_in_slot[-1]) # New end time should not be after end_time of available slots
            model.Add(task_ends[task] - task_starts[task] == duration).OnlyEnforceIf(fits_in_slot[-1]) # Duration should be as it is
            # model.Add(task_starts[task] + original_duration <= slot_end_time).OnlyEnforceIf(fits_in_slot[-1])  # Setting end time constraint keeping the orginal duration of the task as it is
        
        model.Add(sum(fits_in_slot) >= 1)

    # Ensure no tasks overlap
    for task1 in task_starts:
        for task2 in task_starts:
            # print()
            # print(task1,task2)
            if task1 != task2:

                task1_before_task2 = model.NewBoolVar(f"{task1}_before_{task2}")
                task2_before_task1 = model.NewBoolVar(f"{task2}_before_{task1}")

                # model.Add(task_starts[task1] + task_duration[task1] <= task_starts[task2]).OnlyEnforceIf(task1_before_task2)
                # model.Add(task_starts[task2] + task_duration[task2] <= task_starts[task1]).OnlyEnforceIf(task2_before_task1)

                model.Add(task_ends[task1] <= task_starts[task2]).OnlyEnforceIf(task1_before_task2)
                model.Add(task_ends[task2] <= task_starts[task1]).OnlyEnforceIf(task2_before_task1)

                model.AddBoolOr([task1_before_task2, task2_before_task1])

    solver = CpSolver()
    status = solver.Solve(model)

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for task in task_starts:
            schedule = tasks_data[tasks_data['id'] == task]
            start_time_days = solver.Value(task_starts[task])
            end_time_days = solver.Value(task_ends[task])
            start_time = tasks_data['startDate'].min() + pd.to_timedelta(start_time_days, unit='s')
            end_time =  tasks_data['startDate'].min() + pd.to_timedelta(end_time_days, unit='s')
            print(f'Task {task} is scheduled from {start_time} to {end_time}')
            tool_action = f"Scheduled: {(start_time, end_time)}"

            new_log = Log(
                taskMeetingId=task,
                name=schedule['name'].values[0],
                type=schedule['type'].values[0],
                originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task]['startDate'].values[0]),
                originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task]['endDate'].values[0]),
                toolSuggestion=suggestion,
                userAction=user_action,
                toolAction=tool_action
            )

            mock_data = mock_table[mock_table['id'] == task_id].iloc[0]
            current_state = CurrentState(
                id=str(uuid.uuid4()),  # Generate a new UUID for the id
                taskId=mock_data['workspaceId'],
                name=mock_data['name'],
                type=mock_data['type'],
                startDate=int(pd.Timestamp(mock_data['startDate']).timestamp()),
                endDate=int(pd.Timestamp(mock_data['endDate']).timestamp()),
                createdBy=mock_data['createdBy'],   
                actionId=force_table_action_Id
            )

            tool_report = tool_Report(
                id=str(uuid.uuid4()),  # Generate a new UUID for the id
                taskId=new_log.taskMeetingId,
                name=new_log.name,
                type=new_log.type,
                originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]),
                originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]),
                toolSuggestions=new_log.toolSuggestion,
                actionId=force_table_action_Id

            )

        
            after_state = AfterState(
                id=str(uuid.uuid4()),  # Generate a new UUID for the id
                taskId=new_log.taskMeetingId,
                name=new_log.name,
                type=new_log.type,
                originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['startDate'].values[0]),
                originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task_id]['endDate'].values[0]),
                userAction=new_log.userAction,
                toolAction=new_log.toolAction,
                actionId=force_table_action_Id

        )
       
        
            training_logs = TrainingLog(
                taskMeetingId=task,
                name=schedule['name'].values[0],
                type=schedule['type'].values[0],
                originalStartDate=pd.to_datetime(tasks_data[tasks_data['id'] == task]['startDate'].values[0]),
                originalEndDate=pd.to_datetime(tasks_data[tasks_data['id'] == task]['endDate'].values[0]),
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
    else:
        print('No solution found.')