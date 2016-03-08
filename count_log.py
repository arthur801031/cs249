#All imports
import pandas as pd
import numpy as np
from datetime import datetime

#Increment counts of each source type
def incrementsCountsOfSource(server, browser, index, source_vector):

	if source_vector[index] == 'server':
		server += 1
	elif source_vector[index] == 'browser':
		browser += 1

	return [server, browser]

#Increment count of total event types
def incrementsCountsOfEvents(total_events_count_arr, index, event_vector):

	if event_vector[index] == 'problem':
		total_events_count_arr[0] += 1

	elif event_vector[index] == 'video':
		total_events_count_arr[1] += 1

	elif event_vector[index] == 'access':
		total_events_count_arr[2] += 1

	elif event_vector[index] == 'wiki':
		total_events_count_arr[3] += 1

	elif event_vector[index] == 'discussion':
		total_events_count_arr[4] += 1

	elif event_vector[index] == 'navigate':
		total_events_count_arr[5] += 1

	elif event_vector[index] == 'page_close':
		total_events_count_arr[6] += 1

	return total_events_count_arr

#Increment counts of all events for each hour
def incrementCountsOfHours(hours_event_count_arr, index, time_vector):
	hourInTwoDigits = time_vector[index][11:13]
	if hourInTwoDigits[0] == '0':
		hours_event_count_arr[0, hourInTwoDigits[1]] += 1
	else:
		hours_event_count_arr[0, hourInTwoDigits] += 1

	return hours_event_count_arr

#Increment counts of events from Monday to Sunday 
def incrementWeekdayCounts(weekday_count_arr, index, time_vector):

	datestring = time_vector[index][0:10]
	dateobj = datetime.strptime(datestring, '%Y-%m-%d')
	weekday = dateobj.weekday()
	weekday_count_arr[0, weekday] += 1
	return weekday_count_arr

def assignValsToFinalMatrix(finalMatrix, currRow, server, browser, total_events_count_arr, hours_event_count_arr, weekday_count_arr):

	#Assign server count and browser count to 
	#this enrollment_id in finalMatrix
	finalMatrix[currRow, 39] = server 
	finalMatrix[currRow, 40] = browser

	#Assign total_events_count_arr to finalMatrix
	for j in range(len(total_events_count_arr)):
		finalMatrix[currRow, 32 + j] = total_events_count_arr[j]

	#Assign hours_event_count_arr to finalMatrix
	for j in range(len(hours_event_count_arr[0])):
		finalMatrix[currRow, 8 + j] = hours_event_count_arr[0, j]

	for j in range(len(weekday_count_arr[0])):
		finalMatrix[currRow, 1 + j] = weekday_count_arr[0, j]

	return finalMatrix

#Load truth_newsplit_train.csv to determine the row size
#of the finalMatrix
truth_df = pd.read_csv('truth_newsplit_train.csv')
truth_enrollment_id_vector = truth_df['V1']
final_matrix_row_size = len(truth_enrollment_id_vector)

#Load log_newsplit_train.csv
#df = pd.read_csv('log_newsplit_train_small_test.csv')
df = pd.read_csv('log_newsplit_train.csv')
enrollment_vector = df['enrollment_id']
time_vector = df['time']
source_vector = df['source']
event_vector = df['event']

#Initialize all variables
server = 0
browser = 0
currProcessId = enrollment_vector[0]
currRow = 0
finalMatrix = np.zeros((final_matrix_row_size,41))
#Assign the first enrollment id to finalMatrx[0, 0]
finalMatrix[0, 0] = currProcessId

#Strucutre: Problem, video, access, wiki, discussion, navigate, page close
total_events_count_arr = [0, 0, 0, 0, 0, 0, 0]

#Structure: Hour0, Hour1,...,Hour23
hours_event_count_arr = np.zeros((1,24))

#Structure: monday, tuesday, wednesday,..,saturday, sunday
weekday_count_arr = np.zeros((1,7))

for index in range(len(enrollment_vector)):
	
	if currProcessId == enrollment_vector[index]:

		[server, browser] = incrementsCountsOfSource(server, browser, index, source_vector)

		total_events_count_arr = incrementsCountsOfEvents(total_events_count_arr, index, event_vector)

		hours_event_count_arr = incrementCountsOfHours(hours_event_count_arr, index, time_vector)
		
		weekday_count_arr = incrementWeekdayCounts(weekday_count_arr, index, time_vector)

		#If this is the last item in enrollment_vector
		if index == (len(enrollment_vector) - 1):

			currProcessId = enrollment_vector[index]
			finalMatrix[currRow, 0] = currProcessId
			#Assign values to finalMatrix
			assignValsToFinalMatrix(finalMatrix, currRow, server, browser, total_events_count_arr, hours_event_count_arr, weekday_count_arr)

	else:
		#Process another enrollment ID's data

		#Assign values to finalMatrix
		assignValsToFinalMatrix(finalMatrix, currRow, server, browser, total_events_count_arr, hours_event_count_arr, weekday_count_arr)

		########Compute values for this new currProcessId########
		#Reset server and browser count 
		server = 0
		browser = 0
		[server, browser] = incrementsCountsOfSource(server, browser, index, source_vector)

		#Reset total_events_count_arr
		total_events_count_arr = [0, 0, 0, 0, 0, 0, 0]
		total_events_count_arr = incrementsCountsOfEvents(total_events_count_arr, index, event_vector)

		#Reset hours_event_count_arr
		hours_event_count_arr = np.zeros((1,24))
		hours_event_count_arr = incrementCountsOfHours(hours_event_count_arr, index, time_vector)

		#Reset weekday_count_arr
		weekday_count_arr = np.zeros((1,7))
		weekday_count_arr = incrementWeekdayCounts(weekday_count_arr, index, time_vector)

		#Assign this enrollment id to finalMatrix
		currRow += 1
		currProcessId = enrollment_vector[index]
		finalMatrix[currRow, 0] = currProcessId

		if index == (len(enrollment_vector) - 1):
			#Assign values to finalMatrix
			assignValsToFinalMatrix(finalMatrix, currRow, server, browser, total_events_count_arr, hours_event_count_arr, weekday_count_arr)

finalDataframe = pd.DataFrame(finalMatrix)
finalDataframe.columns = ['enrollment_id','MonCount', 'TueCount','WedCount','ThuCount','FriCount','SatCount','SunCount','Hr0Count','Hr1Count','Hr2Count',
        'Hr3Count','Hr4Count','Hr5Count','Hr6Count', 'Hr7Count','Hr8Count','Hr9Count','Hr10Count', 'Hr11Count','Hr12Count','Hr13Count','Hr14Count',
        'Hr15Count','Hr16Count','Hr17Count','Hr18Count', 'Hr19Count','Hr20Count','Hr21Count','Hr22Count', 'Hr23Count','ProCount','VidCount','AccCount',
        'WikiCount','DisCount','NavCount','PageCloCount', 'SerCount','BroCount']

finalDataframe.to_csv('feature1_train.csv')



