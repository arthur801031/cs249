import pandas as pd
import numpy as np
from datetime import datetime

def aggregationCalculate(groupby_keys, finalInputDataframes, columnName, finalOutputDataframes, groupby_key_index):
    #Group by course_id

    #Unique
    finalOutputDataframes['_' + columnName + '_unique'] = finalInputDataframes.groupby(groupby_keys[groupby_key_index])[columnName].transform(pd.Series.nunique)

    #Max
    finalOutputDataframes['_' + columnName + '_max'] = finalInputDataframes.groupby(groupby_keys[groupby_key_index])[columnName].transform(max)

    #Min
    finalOutputDataframes['_' + columnName + '_min'] = finalInputDataframes.groupby(groupby_keys[groupby_key_index])[columnName].transform(min)

    return finalOutputDataframes

def aggregationCalculateTime(sourceEventGroupBy_keys, finalOutputDataframes, log_file, final_matrix_row_size):
    #Max final output features
    #Column 1: server problem ; Column 2: server video ; Column 3: server access ; Column 4: server wiki
    #Column 5: server discussion ; Column 6: server navigate ; Column 7: server page_close ;
    #Column 8: browser problem ; Column 9: browser video ; Column 10: browser access ;
    #Column 11: browser wiki ; Column 12: browser discussion ; Column 13: browser navigate
    #Column 14: browser page_close

    max_final_features = np.zeros((final_matrix_row_size,14))
    min_final_features = np.zeros((final_matrix_row_size,14))
    count_final_features = np.zeros((final_matrix_row_size,14))
    span_final_features = np.zeros((final_matrix_row_size,14))

    # #This is used to generate max feature
    # #Max feature
    # log_file['source_event_max'] = log_file.groupby(sourceEventGroupBy_keys)['time'].transform(max)
    # #Generate time in seconds
    # log_file['max_formated'] = log_file.source_event_max.apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'))
    # log_file['max_delta'] = log_file.max_formated - datetime.utcfromtimestamp(0)
    # log_file['src_evt_max_secs'] = log_file['max_delta'].apply(lambda x: x / np.timedelta64(1, 's'))
    # #Drop the temporary columns
    # del log_file['max_delta']
    # del log_file['max_formated']
    # del log_file['source_event_max']
    # #End of Max feature
    #
    # max_final_features = fillinFeaturesValues(max_final_features, log_file, "src_evt_max_secs")
    #
    # max_final_Dataframe = pd.DataFrame(max_final_features)
    # max_final_Dataframe.columns = ['srv_prb_max','srv_vid_max','srv_aces_max','srv_wiki_max','srv_discus_max','srv_nav_max','srv_page_close_max','brs_prb_max',
    #                           'brs_vid_max','brs_acs_max','brs_wiki_max','brs_dis_max','brs_nav_max','brs_page_close_max']
    # max_final_Dataframe.to_csv('Log_max_features_train.csv')

    # #This is used to generate min feature
    # #Min feature
    # log_file['source_event_min'] = log_file.groupby(sourceEventGroupBy_keys)['time'].transform(min)
    # #Generate time in seconds
    # log_file['min_formated'] = log_file.source_event_min.apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'))
    # log_file['min_delta'] = log_file.min_formated - datetime.utcfromtimestamp(0)
    # log_file['src_evt_min_secs'] = log_file['min_delta'].apply(lambda x: x / np.timedelta64(1, 's'))
    # #Drop the temporary columns
    # del log_file['min_delta']
    # del log_file['min_formated']
    # del log_file['source_event_min']
    #
    # min_final_features = fillinFeaturesValues(min_final_features, log_file, "src_evt_min_secs")
    # min_final_features = pd.DataFrame(min_final_features)
    # min_final_features.columns = ['srv_prb_min','srv_vid_min','srv_aces_min','srv_wiki_min','srv_discus_min','srv_nav_min','srv_page_close_min','brs_prb_min',
    #                           'brs_vid_min','brs_acs_min','brs_wiki_min','brs_dis_min','brs_nav_min','brs_page_close_min']
    # min_final_features.to_csv('Log_min_features_train.csv')

    # #This is used to generate count feature
    # log_file['source_event_count'] = log_file.groupby(sourceEventGroupBy_keys)['time'].transform('count')
    # count_final_features = fillinFeaturesValues(count_final_features, log_file, "source_event_count")
    # count_final_features = pd.DataFrame(count_final_features)
    # count_final_features.columns = ['srv_prb_count','srv_vid_count','srv_aces_count','srv_wiki_count','srv_discus_count','srv_nav_count','srv_page_close_count','brs_prb_count',
    #                           'brs_vid_count','brs_acs_count','brs_wiki_count','brs_dis_count','brs_nav_count','brs_page_close_count']
    # count_final_features.to_csv('Log_count_features_train.csv')


    # #This is used to generate time_span feature
    logMaxcsv = pd.read_csv('Log_max_features_train.csv')
    logMincsv = pd.read_csv('Log_min_features_train.csv')
    logMaxcsvColumnNames = logMaxcsv.columns.values
    logMincsvColumnNames = logMincsv.columns.values
    span_final_features = []
    for index in range(len(logMaxcsvColumnNames)):
        if index == 0:
            #First column is just 0,1,2,3,...,
            pass
        else:
            logMaxColumnVector = logMaxcsv[logMaxcsvColumnNames[index]]
            logMinColumnVector = logMincsv[logMincsvColumnNames[index]]
            eachVectorDataframe = logMaxColumnVector.sub(logMinColumnVector, axis=0)
            span_final_features.append(eachVectorDataframe)

    span_final_features = pd.concat(span_final_features, axis=1)
    span_final_features.columns = ['srv_prb_span','srv_vid_span','srv_aces_span','srv_wiki_span','srv_discus_span',
                                   'srv_nav_span','srv_page_close_span','brs_prb_span', 'brs_vid_span',
                                   'brs_acs_span','brs_wiki_span','brs_dis_span','brs_nav_span',
                                   'brs_page_close_span']

    span_final_features.to_csv('Log_span_features_train.csv')

    #Contatanate multiple dataframes
    #finalOutputDataframes = pd.concat([finalOutputDataframes, finalDataframe], axis=1)
    return finalOutputDataframes

def fillinFeaturesValues(final_features, log_file, columnVal):
    #Loop through every entry in log_file
    source_evt_max_column = log_file[columnVal]
    source_column = log_file['source']
    event_column = log_file['event']
    enrol_column = log_file['enrollment_id']
    currEid = enrol_column[0]
    rowCounter = 0
    for index in range(len(log_file)):

        print index

        if currEid != enrol_column[index]:
            currEid = enrol_column[index]
            rowCounter += 1

        if source_column[index] == "server" and event_column[index] == "problem":
            final_features[rowCounter, 0] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "video":
            final_features[rowCounter, 1] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "access":
            final_features[rowCounter, 2] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "wiki":
            final_features[rowCounter, 3] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "discussion":
            final_features[rowCounter, 4] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "navigate":
            final_features[rowCounter, 5] = source_evt_max_column[index]

        elif source_column[index] == "server" and event_column[index] == "page_close":
            final_features[rowCounter, 6] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "problem":
            final_features[rowCounter, 7] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "video":
            final_features[rowCounter, 8] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "access":
            final_features[rowCounter, 9] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "wiki":
            final_features[rowCounter, 10] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "discussion":
            final_features[rowCounter, 11] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "navigate":
            final_features[rowCounter, 12] = source_evt_max_column[index]

        elif source_column[index] == "browser" and event_column[index] == "page_close":
            final_features[rowCounter, 13] = source_evt_max_column[index]

    return final_features

#Keys for groupby
groupby_keys = ['course_id', 'username', 'enrollment_id']
sourceEventGroupBy_keys = ['enrollment_id', 'source', 'event']

#Import data into finalInputDataframes
#Import my 40 features
feature1 = pd.read_csv('feature1_train.csv')

#Import 23 features
feature2 = pd.read_csv('feature2_train.csv')
del feature2['enrollment_id']

enrollmentInfo = pd.read_csv('enrollment_newsplit_train.csv')
del enrollmentInfo['enrollment_id']
sumDataframes = [feature1, feature2, enrollmentInfo]
finalInputDataframes = pd.concat(sumDataframes, axis=1)

finalOutputDataframes = finalInputDataframes
finalInputColumnNames = finalInputDataframes.columns.values
excludeColumnsArr = ['username', 'course_id', 'Unnamed: 0', 'enrollment_id']
#log_file = pd.read_csv('log_newsplit_train.csv')
log_file = pd.read_csv('log_newsplit_train_small_test.csv')

#Get row size of enrollment_vector
truth_df = pd.read_csv('truth_newsplit_train.csv')
truth_enrollment_id_vector = truth_df['V1']
final_matrix_row_size = len(truth_enrollment_id_vector)

#Group by course_id || username || enrollment_id
#This feature generation may be useless...
for groupByindex in range(len(groupby_keys)):
    for index in range(len(finalInputColumnNames)):
        if any(finalInputColumnNames[index] in s for s in excludeColumnsArr):
            print finalInputColumnNames[index]
            # Do not execute anything because they are not numerical
        else:
            #finalOutputDataframes = aggregationCalculate(groupby_keys, finalInputDataframes, finalInputColumnNames[index], finalOutputDataframes, groupByindex)
            pass

finalOutputDataframes = aggregationCalculateTime(sourceEventGroupBy_keys, finalOutputDataframes, log_file, final_matrix_row_size)

#newData = finalOutputDataframes[0:10]
#finalOutputDataframes.to_csv('FinalFeatures.csv')

