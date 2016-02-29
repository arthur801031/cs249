#All imports
import pandas as pd
import numpy as np

#Extract counts of events from Monday to Sunday 
#Extract counts of all events for each hour

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

#Load truth_newsplit_train.csv to determine the row size
#of the finalMatrix
truth_df = pd.read_csv('truth_newsplit_train.csv')
truth_enrollment_id_vector = truth_df['V1']
final_matrix_row_size = len(truth_enrollment_id_vector)

#Load log_newsplit_train.csv
df = pd.read_csv('log_newsplit_train_small_test.csv')
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

for index in range(len(enrollment_vector)):
	
	if currProcessId == enrollment_vector[index]:

		[server, browser] = incrementsCountsOfSource(server, browser, index, source_vector)

		total_events_count_arr = incrementsCountsOfEvents(total_events_count_arr, index, event_vector)
		
	else:
		#Process another enrollment ID's data

		#Assign server count and browser count to 
		#this enrollment_id in finalMatrix
		finalMatrix[currRow, 39] = server 
		finalMatrix[currRow, 40] = browser

		#Assign total_events_count_arr to finalMatrix
		for j in range(len(total_events_count_arr)):
			finalMatrix[currRow, 32 + j] = total_events_count_arr[j]



		########Compute values for this new currProcessId########
		#Reset server and browser count 
		server = 0
		browser = 0
		[server, browser] = incrementsCountsOfSource(server, browser, index, source_vector)

		#Reset total_events_count_arr
		total_events_count_arr = [0, 0, 0, 0, 0, 0, 0]
		total_events_count_arr = incrementsCountsOfEvents(total_events_count_arr, index, event_vector)


		#Assign this enrollment id to finalMatrix
		currRow += 1
		currProcessId = enrollment_vector[index]
		finalMatrix[currRow, 0] = currProcessId

print finalMatrix[0, :]
print finalMatrix[1, :]



