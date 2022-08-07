import pandas as pd
agent_log_df = pd.read_csv(r"C:\Users\user\Downloads\AgentLogingReport.csv")
# print(agent_log_df.head())

agent_performance_df = pd.read_csv(r"C:\Users\user\Downloads\AgentPerformance.csv")
# print(agent_performance_df.head())
# print(agent_performance_df.dtypes)
agent_performance_df['Date'] = agent_performance_df['Date'].apply(pd.to_datetime,errors='coerce')
agent_log_df['Date'] = agent_log_df['Date'].apply(pd.to_datetime,errors='coerce')
# print(agent_log_df.dtypes)
# print(agent_performance_df.dtypes)
# Total working days for each agent
agent_performance_df['week']= agent_performance_df['Date'].dt.isocalendar().week
# print(agent_performance_df)

# ---------------------------Questions Start From Here----------------------------------

# 1 Find out there average rating on weekly basis keep this in mind that they take two day leave in a week
print(agent_performance_df.groupby ('week')['Average Rating'].mean())
print(agent_performance_df.columns)
# 2. Total working days for each agents
agent_performance_df.groupby('Agent Name')['Date'].count()
# 3 Total query have taken
print(agent_performance_df.groupby('Agent Name')['Total Chats'].sum())
# 4 Total Feedback that you have received
print(agent_performance_df.groupby('Agent Name')['Total Feedback'].sum())
# 5 Agent name who have average rating between 3.5 to 4
print(agent_performance_df[(agent_performance_df['Average Rating'] >= 3.5) & (agent_performance_df['Average Rating'] <=4)]['Agent Name'].unique())
# 6 Agent name who have rating less than 3.5
print(agent_performance_df[agent_performance_df['Average Rating'] < 3.5 ]['Agent Name'].unique())
# 7 Agent name who have rating more then 4.5
print(agent_performance_df[agent_performance_df['Average Rating'] > 4.5]['Agent Name'].unique())
# 8 how many feedback agents have received more than 4.5 average
print(agent_performance_df[agent_performance_df['Average Rating'] > 4.5].groupby('Agent Name')['Total Feedback'].sum())
# 9 Average weekly response time for each agent
print(agent_performance_df.dtypes)
print(agent_performance_df.columns)
agent_performance_df['new average response time'] = agent_performance_df['Date'].astype(str)+' '+agent_performance_df['Average Response Time'].astype(str)
agent_performance_df['new average response time'] = pd.to_datetime(agent_performance_df['new average response time'])
print(agent_performance_df.groupby(['week','Agent Name'])['new average response time'].mean())
# 10 Average weekly resolution time for each agent
agent_performance_df['New Average Resolution Time'] = agent_performance_df['Date'].astype(str)+' '+agent_performance_df['Average Resolution Time'].astype(str)
agent_performance_df['New Average Resolution Time'] = pd.to_datetime(agent_performance_df['New Average Resolution Time'])
print(agent_performance_df)
print(agent_performance_df.groupby(['week','Agent Name'])['New Average Resolution Time'].mean())
# 11 list of all agents name
print(agent_performance_df['Agent Name'].unique())
# 12 percentage of chat on which they hace recived a feedback
agent_performance_df['Feedback Percentage'] = agent_performance_df['Total Feedback']/agent_performance_df['Total Chats']*100
print(agent_performance_df['Feedback Percentage'].mean())
# 13 total contribution hour for each and every agents weekly basis
agent_log_df['New Duration'] = pd.to_timedelta(agent_log_df['Duration'])
agent_log_df['week'] = agent_log_df['Date'].dt.isocalendar().week
print(agent_log_df.groupby(['Agent','week'])['New Duration'].sum())
# 14 total percentage of active hours for a month
print(agent_log_df.groupby('Agent')['New Duration'].mean())
