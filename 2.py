from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, declarative_base
from urllib.parse import quote_plus

# Configuration for the database connection
config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root@123',
    'database': 'scheduler',
    'port': 3306
}

# Connect to the database
try:
    encoded_password = quote_plus(config['password'])
    DATABASE_URI = f'mysql+pymysql://{config["user"]}:{encoded_password}@{config["host"]}:{config["port"]}/{config["database"]}'
    engine = create_engine(DATABASE_URI)
except SQLAlchemyError as e:
    print(f"An error occurred while connecting to the database: {e}")

Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Get data and convert to DataFrame
mock_table_query = """
    SELECT id, workspaceId, name, type, from_unixtime(floor(startDate/1000)) AS startDate, 
    from_unixtime(floor(endDate/1000)) AS endDate, assignedTo, userIds, boxingId, 
    isContinuous, isLocked, isComplex, createdBy, isDeleted, isSimpleTask, isCompleted 
    FROM mockData;
"""
mock_table = pd.read_sql(mock_table_query, engine)
mock_table = mock_table.sort_values('startDate')

organization_data_query = """SELECT id, userId, organizationName, email, contact, street, city, state, country, zipcode FROM organization"""
organization_data = pd.read_sql(organization_data_query, engine)

organization_user_data_query = """SELECT id, organizationId, userId, isActive, roleId, specilization FROM organizationUserList"""
organization_user_data = pd.read_sql(organization_user_data_query, engine)

user_table_query = """SELECT id, firstName FROM user"""
user_table = pd.read_sql(user_table_query, engine)
print(mock_table)

# Prompt user for taskID and fetch relevant data
task_id = input("Please enter the taskID from the mock_data table: ")
user_input = input("\nWould you like to add or assign the task to another user? (yes/no): ").strip().lower()
print(organization_user_data)

if task_id:
    task_data = mock_table[mock_table['id'] == task_id]
    if not task_data.empty:
        start_date = task_data['startDate'].values[0]
        end_date = task_data['endDate'].values[0]
        assigned_to = task_data['assignedTo'].values[0]
        task_name = task_data['name'].values[0]
        workspace_id = task_data['workspaceId'].values[0]
        
        # Check the organization associated with the workspace
        workspace_data_query = f"SELECT * FROM workspace WHERE id = '{workspace_id}'"
        workspace_data = pd.read_sql(workspace_data_query, engine)
        if not workspace_data.empty:
            organization_id = workspace_data['organizationId'].values[0]
            organization_info = organization_data[organization_data['id'] == organization_id]
            if not organization_info.empty:
                
                # Find users with specializations that match words in the task name
                task_words = set(task_name.lower().split())
                matching_users = []

                for _, user in organization_user_data[organization_user_data['organizationId'] == organization_id].iterrows():
                    specialization_words = set(user['specilization'].lower().split())
                    if task_words & specialization_words:
                        matching_users.append(user)

                assigned_user_ids = assigned_to.split(',') if assigned_to else []
            
                if user_input == "yes":
                    if matching_users:
                        print(f"\nUsers with specializations matching the task '{task_name}':")
                        nearest_user = None
                        nearest_date_diff = timedelta.max

                        for user in matching_users:
                            if str(user['userId']) not in assigned_user_ids:
                                user_info = user_table[user_table['id'] == user['userId']]
                                if not user_info.empty:
                                    print(f"User ID: {user['userId']}, Name: {user_info['firstName'].values[0]}, Specialization: {user['specilization']}")
                                    # Check if the user is free during the current task's start and end dates
                                    other_tasks = mock_table[mock_table['assignedTo'].fillna('').str.contains(str(user['userId']), na=False)]
                                    task_dates = [(task['startDate'], task['endDate']) for _, task in other_tasks.iterrows()]
                                    is_free = all(not (start_date <= other_end_date and end_date >= other_start_date) for other_start_date, other_end_date in task_dates)
                                    
                                    if is_free:
                                        min_date_diff = timedelta.max
                                        for other_start_date, other_end_date in task_dates:
                                            date_diff = abs(other_end_date - end_date)
                                            if date_diff < min_date_diff:
                                                min_date_diff = date_diff

                                        if min_date_diff < nearest_date_diff:
                                            nearest_date_diff = min_date_diff
                                            nearest_user = user
                                            
                        if nearest_user is not None:                                                                                                                                                           
                            user_info = user_table[user_table['id'] == nearest_user['userId']]
                            print(f"\nThe earliest available user is: {nearest_user['userId']}")
                            if not user_info.empty:
                                print(f"Name: {user_info['firstName'].values[0]}, Specialization: {nearest_user['specilization']}")
                                print(f"\nTask {task_id} from {pd.Timestamp(start_date)} to {pd.Timestamp(end_date)} has been assigned to User ID: {nearest_user['userId']}.")
                        else:
                            print("\nNo available user found.")


