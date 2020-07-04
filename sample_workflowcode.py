# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 11:26:33 2020

@author: kamalesh.pradhan
"""


from datetime import timedelta
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import os,sys
from openpyxl import load_workbook


os.chdir(r'C:\Users\kamalesh.pradhan\OneDrive - Accenture\UPS\QuickHire\Jul')
workflow = pd.read_excel('tblWorkflowHistory.xlsx')

application = pd.read_excel('tblApplications.xlsx')
application=application[['application_id', 'application_status']]
# workflow['Readyfor_hire_hiredgems'] =np.where((workflow['state']=='Ready For Hire' ) & (workflow['event']=='Hired In GEMS'),1,0 )
workflow = workflow.merge(application,on='application_id',how='left')
workflow = workflow.rename(columns={'application_status':'Application Status'})

# workflow_1 = workflow[workflow['Readyfor_hire_hiredgems']==1]
# workflow_1 = workflow_1.drop_duplicates(['application_id'])
# workflow_unq = workflow_unq.merge(workflow_1[['application_id','Readyfor_hire_hiredgems']],on='application_id',how='left')

workflow_old = pd.read_excel(r'C:\Users\kamalesh.pradhan\OneDrive - Accenture\UPS\QuickHire\tblWorkflowHistory.xlsx')    

workflow_unq = workflow.sort_values(['application_id','timestamp'],ascending=[True,True])
workflow_old_unq = workflow_old.sort_values(['Application ID','Timestamp'],ascending=[True,True])

workflow_unq = workflow_unq.drop_duplicates(['application_id'])
workflow_old_unq = workflow_old_unq.drop_duplicates(['Application ID'])

workflow_unq['year'] = workflow_unq['timestamp'].dt.year
workflow_unq['month'] = workflow_unq['timestamp'].dt.month

workflow_old_unq['year'] = workflow_old_unq['Timestamp'].dt.year
workflow_old_unq['month'] = workflow_old_unq['Timestamp'].dt.month

# workflow_old_unq = workflow_old_unq[workflow_old_unq['Timestamp']>=pd.to_datetime()]
workflow_unq = workflow_unq[workflow_unq['timestamp']>=pd.to_datetime("2020-01-01")]
workflow_unq_jan_mar = workflow_unq[workflow_unq['timestamp']<pd.to_datetime("2020-04-1")]
workflow_unq_apr_jun = workflow_unq[workflow_unq['timestamp']>=pd.to_datetime("2020-04-01")]
workflow_unq_apr_jun = workflow_unq_apr_jun[workflow_unq_apr_jun['timestamp']<pd.to_datetime("2020-07-01")]

old=workflow_old_unq.groupby(['year','month'],as_index=False).agg({'application_id':'count'})

old = workflow_old_unq.pivot_table(index=['year','month'], values=['Application ID'],aggfunc = pd.Series.nunique).reset_index()
new = workflow_unq.pivot_table(index=['year','month'], values=['application_id'],aggfunc = pd.Series.nunique).reset_index()


workflow = workflow.rename(columns={'application_id':'Application ID','timestamp':'Timestamp','state':'State','event':'Event'})

# workflow = workflow.sort_values(['Application ID', 'State',  'Timestamp'],ascending = [True,True,False])
# workflow = workflow.drop_duplicates(['Application ID', 'State'])
# workflow = workflow[~workflow['State'].isin(['Process Terminated'])]

# workflow_old1 = workflow_old[workflow_old['Timestamp'].dt.date>=pd.to_datetime('2020-01-01').date()]
# workflow=workflow_old1.copy()


workflow['RFstatus'] = np.where(workflow['State'].isin(["Initial State",
"Ready To Fill Out Application",
"Prev Employee - Neg Rehire - Process Terminated",
"Pre-Application Process Terminated",
"Pre-Interview - Candidate Interviewed Same Job Group/District",
'Current Employee - Process Terminated']),'Application',
                              np.where(workflow['State'].isin(["Assessment In Progress",
"Process Terminated - Assessment Not Completed",
"Process Terminated - Assessment",
"Filling Out Application - Pre Assessment Result",
"Accommodation In Progress",
"Process Terminated - Accommodation",
'Process Terminated - EJF']),'Assessment',
                                       np.where(workflow['State'].isin(["Provisional Job Offer Accepted",
"Filling Out Application - Post Assessment Result",
"Process Terminated - Provisional Job Offer Declined",
"Express Hire - Processing Docs",
"Processed Not First Day Scheduled",
"First Day Scheduled",
"Pre-Ready For Hire",
"Ready For Hire",
'Hired Elsewhere - Process Terminated',
'Pending Selection of First Day of Work']),'Provisional Job Offer',
                                                         np.where(workflow['State'].isin(["Filling out Background Check Compliance Docs",
"SSN Matches Existing - Merge Required",
"Background Check Request Complete",
"Awaiting BG Check Result",
"WFPM BG/Employ Approval Required",
"Ineligible BG Check",
"Completed Background Check Compliance Docs",
'Biometrics Appt Complete',
'Biometrics Appt Selection Pending',
'In Biometrics Appt',
'No-Show For Biometrics Appt',
'Scheduled Biometrics Appt']),'Background Check',
                                                                  np.where(workflow['State'].isin(["First Day No-Show",
"First Day Scheduled, Not Hired",
"Hired"]),'Hired','None')))))
                     

workflow['RFstatus_order'] = np.where(workflow['RFstatus']=='Application',1,
                                      np.where(workflow['RFstatus']=='Assessment',2,
                                                        np.where(workflow['RFstatus']=='Provisional Job Offer',3,
                                                                 np.where(workflow['RFstatus']=='Background Check',4,
                                                                          np.where(workflow['RFstatus']=='Hired',5,100)))))                       

                                                            
# workflow = workflow.merge(application_unq_appid_hire[['Application ID', 'Hiring Plan ID']],on='Application ID',how='left')
# workflow = workflow.merge(application_unq_appid[['Application ID', 'Application Status']],on='Application ID',how='left')
# workflow = workflow.merge(plandata,on='Hiring Plan ID',how='left')

# workflow.to_excel('testworkflow.xlsx')
# workflow['RFstatus_Application'] = np.where(workflow['Application Status'].isin(["Prev Employee - Neg Rehire - Process Terminated",
# "Ready To Fill Out Application",
# "Pre-Application Process Terminated",
# "Current Employee - Process Terminated",
# "Filling Out Application - Pre Assessment Result"]),'Application',
#                               np.where(workflow['Application Status'].isin(["Assessment In Progress",
# "Process Terminated - Assessment",
# "Process Terminated - Assessment Not Completed",
# "Process Terminated - EJF",
# "Process Terminated - Accommodation",
# "Accommodation In Progress"]),'Assessment',
#                                        np.where(workflow['Application Status'].isin(["Express Hire - Processing Docs",
# "First Day Scheduled",
# "Ready For Hire",
# "Filling Out Application - Post Assessment Result",
# "Pre-Ready For Hire",
# "Process Terminated - Provisional Job Offer Declined",
# "Processed Not First Day Scheduled"]),'Provisional Job Offer',
#                                                          np.where(workflow['Application Status'].isin(["Ineligible BG Check",
# "Awaiting BG Check Result",
# "Filling out Background Check Compliance Docs",
# "WFPM BG/Employ Approval Required",
# "SSN Matches Existing - Merge Required"]),'Background Check',
#                                                                   np.where(workflow['Application Status'].isin(["Hired",
# "First Day No-Show",
# "First Day Scheduled, Not Hired"]),'Hired','None')))))

workflow['QuickHire_Flag'] = np.where(workflow['State'].isin(
["On-Site Application Scheduled",
"On-Site Application No-Show",
"WFPM Rehire Denied",
"Inquiry-Awaiting Recruiter Application Invitation",
"Ready To Fill Out App - WFPM Approved",
"Filling Out Application",
"Application Complete",
"WFPM Rehire Approval Required",
"Candidate Selected Not Scheduled",
"Scheduled For Interview",
"Scheduled For Tour",
"Interview No-Show",
"Tour No-Show",
"Ready For Interview",
"In Interview",
"In Interview - Reviewing Application",
"In Tour",
"Documents Reviewed",
"Tour Complete",
"In Interview - More Documents Required",
"Interview Acceptable - Not Job Offered",
"Processing Scheduled",
"Processing No-Show",
"WFPM Rehire Approval Req - PostInt",
"Filling Out Processing Documents",
"Pre-Ready For Hire - Converted Data",
"WFPM Rehire Denied - Post Int",
'Pre-Interview - Candidate Interviewed Same Job Group/District']),1,0)


nonquickhire_app = workflow.groupby(['Application ID'],as_index=False).agg({'QuickHire_Flag':sum})
nonquickhire_app=nonquickhire_app[nonquickhire_app['QuickHire_Flag']>0]

workflow['QuickHire_NonQuickHire'] = np.where(workflow['Application ID'].isin(list(nonquickhire_app['Application ID'].values)),'Non_QuickHire','QuickHire')
QHN = pd.pivot_table(workflow,index=['QuickHire_NonQuickHire'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()


workflow['New_RFStatus'] = np.where((workflow['State']=='Ready To Fill Out Application') & (workflow['Event']=='Assessment Started'),'Assessment',
                                              np.where((workflow['State']=='First Day Scheduled') & (workflow['Event']=='No Show'),'First Day No-Show',
                                                       np.where((workflow['State']=='First Day Scheduled') & (workflow['Event']=='Scheduled - Not Hired'),'First Day Scheduled, Not Hired',
                                                                np.where(workflow['State']=='First Day No-Show','First Day No-Show',
                                                                         np.where(workflow['State']=='First Day Scheduled, Not Hired','First Day Scheduled, Not Hired',
                                                                                  np.where(workflow['State']=='Hired','Hired',workflow['RFstatus']))))))

workflow['New_RFStatus1'] = np.where(workflow['New_RFStatus'].isin(['First Day Scheduled, Not Hired','First Day No-Show']),'Hired',workflow['New_RFStatus'])
# workflow['New_RFStatus1'] = np.where((workflow['State'].isin(['Ready For Hire'])) & (workflow['Event'].isin(['Hired In GEMS'])) ,'Hired',workflow['New_RFStatus1'])
workflow['New_RFStatus1'] = np.where((workflow['State'].isin(['Ready For Hire'])) & (workflow['Event'].isin(['Hired In GEMS'])) & (workflow['Application Status'].isin(['Hired'])),'Hired',workflow['New_RFStatus1'])

# chk = workflow_quickhire[workflow_quickhire['RFstatus']=='Hired'] 
# chk = chk[chk['Application Status']=='Hired']
# chk = chk[chk['State']=='Ready For Hire']       

workflow = workflow.rename(columns={'New_RFStatus':'RFstatus_old'})

workflow = workflow.drop('RFstatus',1).rename(columns={'New_RFStatus1':'RFstatus'})
   
workflow_quickhire= workflow.copy()
workflow_quickhire['Impute_date'] = pd.to_datetime(workflow_quickhire['Timestamp'].max())
workflow_quickhire = workflow_quickhire.copy()
workflow_quickhire.sort_values(['Application ID', 'State', 'Event', 'Timestamp'], inplace=True) 
workflow_quickhire = workflow_quickhire.drop_duplicates(['Application ID', 'State', 'Event', 'Timestamp'])
workflow_quickhire = workflow_quickhire[workflow_quickhire['QuickHire_NonQuickHire'] == 'QuickHire']
workflow_quickhire = workflow_quickhire[~((workflow_quickhire['State'] == 'Hired') & (workflow_quickhire['Event'] == 'Termination Received from GEMS')) ]
workflow_quickhire = workflow_quickhire[~(workflow_quickhire['RFstatus'] == 'None') ]

# Filter out for Non-QuickHire
workflow_quickhire = workflow_quickhire[~workflow_quickhire['Application ID'].isin(list(nonquickhire_app['Application ID'].values))]

# workflow.to_excel('testworkflow.xlsx')

# workflow_quickhire[['Hires Per IE Plan', 'Additional Hires','Slots Taken']] =workflow_quickhire[['Hires Per IE Plan', 'Additional Hires','Slots Taken']].astype(float)
# workflow_quickhire['Total_Hire'] = workflow_quickhire['Hires Per IE Plan'] +workflow_quickhire['Additional Hires']

workflow_quickhire_copy =workflow_quickhire.copy()
##  Person vs RFStatus
workflow_quickhire['dummy'] = 1
person_vs_Rfstatus = pd.pivot_table(workflow_quickhire,index=['Application ID'],columns='RFstatus',values='dummy',aggfunc =pd.Series.nunique).reset_index()


## Avg Time Taken
Avgtimetakenmax = pd.pivot_table(workflow_quickhire,index=['Application ID','RFstatus'],values='Timestamp',aggfunc =np.max).reset_index()
Avgtimetakenmin = pd.pivot_table(workflow_quickhire,index=['Application ID','RFstatus'],values='Timestamp',aggfunc =np.min).reset_index()
Avgtimetaken = Avgtimetakenmax.rename(columns={'Timestamp':'Max_Timestamp'}).merge(Avgtimetakenmin.rename(columns={'Timestamp':'Min_Timestamp'}),on=['Application ID', 'RFstatus'])
# Avgtimetaken = Avgtimetaken['Max_Timestamp']-

# Avgtimetaken = Avgtimetaken.set_index(['Application ID'])
# Avgtimetaken['lag'] = Avgtimetaken.groupby(Avgtimetaken.index)['Min_Timestamp'].shift(-1)
# Avgtimetaken = Avgtimetaken.reset_index()
# Avgtimetaken['lag'] = np.where(Avgtimetaken['lag'].isnull(),Avgtimetaken['Max_Timestamp'],Avgtimetaken['lag'])
Avgtimetaken['Timetaken'] = (Avgtimetaken['Max_Timestamp'] - Avgtimetaken['Min_Timestamp'])/ np.timedelta64(1, 's')
Avgtimetaken['Timetaken_hours'] = Avgtimetaken['Timetaken']/3600

## Time by Employee at each step
Avgtimetaken_emp_rfstatus = pd.pivot_table(Avgtimetaken,index=['Application ID'],columns='RFstatus',values='Timetaken_hours',aggfunc =np.max).reset_index()
Avgtimetaken_emp_rfstatus = Avgtimetaken_emp_rfstatus.rename(columns={'Application':'Application_timetaken','Assessment':'Assessment_timetaken','Provisional Job Offer':'Provisional_Job_Offer_timetaken','Background Check':'Background Check_timetaken','Hired':'Hired_timetaken'})
Avgtimetaken_emp_rfstatus = Avgtimetaken_emp_rfstatus.fillna(0)
## 

Avgtimetaken_state = Avgtimetaken.groupby(['RFstatus'],as_index=False).agg({'Timetaken_hours':np.mean})

#############

## Min-Max Time by status
Maxtime = pd.pivot_table(workflow_quickhire,index=['Application ID'],columns='RFstatus',values='Timestamp',aggfunc =np.max).reset_index()
Maxtime = Maxtime.rename(columns={'Application':'Application_maxtime','Assessment':'Assessment_maxtime','Provisional Job Offer':'Job_Offer_maxtime','Background Check':'BGC_maxtime','Hired':'Hired_maxtime'})
Mintime = pd.pivot_table(workflow_quickhire,index=['Application ID'],columns='RFstatus',values='Timestamp',aggfunc =np.min).reset_index()
Mintime = Mintime.rename(columns={'Application':'Application_mintime','Assessment':'Assessment_mintime','Provisional Job Offer':'Job_Offer_mintime','Background Check':'BGC_mintime','Hired':'Hired_mintime'})
Emp_minmaxtime = Maxtime.merge(Mintime,on=['Application ID'],how='left')
# Emp_minmaxtime = Emp_minmaxtime.drop(['None_x','None_y'],1)

workflow_quickhire = workflow_quickhire.merge(Emp_minmaxtime,on=['Application ID'],how='left')
workflow = workflow.merge(Emp_minmaxtime,on=['Application ID'],how='left')

workflow = workflow.merge(Avgtimetaken_emp_rfstatus,on='Application ID',how='left')
workflow_quickhire = workflow_quickhire.merge(Avgtimetaken_emp_rfstatus,on='Application ID',how='left')
workflow_quickhire = workflow_quickhire.merge(person_vs_Rfstatus,on='Application ID',how='left')

###########

wf_p = workflow_quickhire.copy()
wf_p = wf_p.rename(columns={'Application_mintime':'min_app_date','Application_maxtime':'max_app_date',
                             'Assessment_mintime':'min_asses_date','Assessment_maxtime':'max_assess_date',
                             'Job_Offer_mintime':'min_job_date','Job_Offer_maxtime':'max_job_date',
                             'BGC_mintime':'min_bgc_date','BGC_maxtime':'max_bgc_date',
                             'Hired_mintime':'min_hired_date','Hired_maxtime':'max_hired_date'})      
## REad Mapping
mapping_state_event = pd.read_excel('Workflowstatus_state_event_mapping_jul2020.xlsx',sheet_name='State_Event Mapping')
mapping_state_event = mapping_state_event[mapping_state_event['ActionType'].notnull()]

wf_p_with_maxmin  = pd.DataFrame()
for appid in wf_p['Application ID'].unique():
    application = wf_p[wf_p['Application ID']==appid]
    application = application.sort_values(['Application ID','Timestamp'])
    maxstate = application['RFstatus_order'].max()
    # Identify state and event for application and get max_date_decision
    # lateststateevent = mapping_state_event[mapping_state_event['State']==application.iloc[-1,:]['State']][mapping_state_event['Event']==application.iloc[-1,:]['Event']]['ActionType'].values[0]
    max_RFStatus = application.iloc[-1,:]['RFstatus']   
    # for status in ['Application','Assessment','Background Check','Provisional Job Offer']
    application['min_app_date_final'] = application['min_app_date'] 
    if max_RFStatus=='Application':        
        max_date_decision = mapping_state_event[mapping_state_event['State']==application.iloc[-1,:]['State']][mapping_state_event['Event']==application.iloc[-1,:]['Event']]['ActionType'].values[0]
        application['max_app_date_final'] = np.where(max_date_decision=='Impute',application['Impute_date'],application['max_app_date'])
    else:
        application['max_app_date_final'] = application['min_asses_date']
    
    application['min_assess_date_final'] = application['min_asses_date'] 
    if max_RFStatus=='Assessment':
        max_date_decision = mapping_state_event[mapping_state_event['State']==application.iloc[-1,:]['State']][mapping_state_event['Event']==application.iloc[-1,:]['Event']]['ActionType'].values[0]
        application['max_assess_date_final'] = np.where(max_date_decision=='Impute',application['Impute_date'],application['max_assess_date'])
    else:
        application['max_assess_date_final'] = min(application['min_bgc_date'].values[0],application['min_job_date'].values[0])
        
    application['min_bgc_date_final'] = application['min_bgc_date'] 
    if max_RFStatus=='Background Check':
        max_date_decision = mapping_state_event[mapping_state_event['State']==application.iloc[-1,:]['State']][mapping_state_event['Event']==application.iloc[-1,:]['Event']]['ActionType'].values[0]
        application['max_bgc_date_final'] = np.where(max_date_decision=='Impute',application['Impute_date'],application['max_bgc_date'])
    else:
        application['max_bgc_date_final'] = application['max_bgc_date']
    
    application['min_job_date_final'] = application['min_job_date'] 
    if max_RFStatus=='Provisional Job Offer':
        max_date_decision = mapping_state_event[mapping_state_event['State']==application.iloc[-1,:]['State']][mapping_state_event['Event']==application.iloc[-1,:]['Event']]['ActionType'].values[0]
        application['max_job_date_final'] = np.where(max_date_decision=='Impute',application['max_bgc_date_final'],application['max_job_date'])
    else:
        application['max_job_date_final'] = application['max_job_date']
        
    wf_p_with_maxmin = wf_p_with_maxmin.append(application)
        
# wf_p_with_maxmin.to_excel('wf_p_with_maxmin_v1.xlsx',index=False)

# wf_p_with_maxmin = pd.read_excel('wf_p_with_maxmin_v1.xlsx')        

wf_p_with_maxmin['max_assess_date_final'] = np.where(wf_p_with_maxmin['max_assess_date_final'].isnull(),wf_p_with_maxmin['max_assess_date'],wf_p_with_maxmin['max_assess_date_final'])
wf_p_with_maxmin['max_bgc_date_final'] = np.where(wf_p_with_maxmin['max_bgc_date_final'].isnull(),wf_p_with_maxmin['max_bgc_date'],wf_p_with_maxmin['max_bgc_date_final'])
wf_p_with_maxmin['max_job_date_final']= np.where(wf_p_with_maxmin['max_job_date_final'].isnull(),wf_p_with_maxmin['max_job_date'],wf_p_with_maxmin['max_job_date_final'])

wf_p_with_maxmin['Application_timetaken']=(wf_p_with_maxmin['max_app_date_final'] -wf_p_with_maxmin['min_app_date_final'])/ np.timedelta64(1, 's')
wf_p_with_maxmin['Assessment_timetaken']=(wf_p_with_maxmin['max_assess_date_final'] -wf_p_with_maxmin['min_assess_date_final'])/ np.timedelta64(1, 's')
wf_p_with_maxmin['Background Check_timetaken']=(wf_p_with_maxmin['max_bgc_date_final'] -wf_p_with_maxmin['min_bgc_date_final'])/ np.timedelta64(1, 's')
wf_p_with_maxmin['Provisional_Job_Offer_timetaken']=(wf_p_with_maxmin['max_job_date_final'] -wf_p_with_maxmin['min_job_date_final'])/ np.timedelta64(1, 's')

####################

wf_p_with_maxmin['App_assess_timetaken'] = wf_p_with_maxmin['Application_timetaken'].fillna(0)+wf_p_with_maxmin['Assessment_timetaken'].fillna(0)
wf_p_with_maxmin['App_assess_bgc_timetaken'] = wf_p_with_maxmin['Application_timetaken'].fillna(0)+wf_p_with_maxmin['Assessment_timetaken'].fillna(0)+wf_p_with_maxmin['Background Check_timetaken'].fillna(0)
wf_p_with_maxmin['App_assess_job_bgc_timetaken'] = wf_p_with_maxmin['Application_timetaken'].fillna(0)+wf_p_with_maxmin['Assessment_timetaken'].fillna(0)+wf_p_with_maxmin['Background Check_timetaken'].fillna(0)+wf_p_with_maxmin['Provisional_Job_Offer_timetaken'].fillna(0)

## Deciling by Different Status Time Taken   
def rankorder(data,variable,group):
    
    data = data.drop_duplicates('Application ID')
    data = data.sort_values([variable],ascending=[True])
    data[variable+'_rank'] = data[variable].rank(method='first')
    data[variable+'_decile'] = pd.qcut(data[variable+'_rank'].values, group).codes
    
    # data=  data[data['Provisional Job Offer']==1]
    app = pd.pivot_table(data,index=[variable+'_decile'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
    hired = pd.pivot_table(data[data['Hired']==1],index=[variable+'_decile'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
    
    appmintime = pd.pivot_table(data,index=[variable+'_decile'],values=variable,aggfunc =np.min).reset_index()
    appmaxtime = pd.pivot_table(data,index=[variable+'_decile'],values=variable,aggfunc =np.max).reset_index()
    var = variable+'_decile'
    app = app.merge(hired.rename(columns={'Application ID':'# Hired'}),on=var,how='left')
    app['%Hired'] = app['# Hired']/app['Application ID']
    app = app.merge(appmintime.rename(columns={variable:'Min Time'}).rename(columns={'Application ID':'# Application'}),on=var,how='left')
    app = app.merge(appmaxtime.rename(columns={variable:'Max Time'}),on=var,how='left')
    app = app.rename(columns={'Application ID':'# Application'})
    return app

    ## 

wf_p_with_maxmin['App_assess_bgc_timetaken_grp'] = np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=0.5,'30mins',
                                                            # np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=0.5,'15-30mins',
                                                            np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=1,'30mins-1Hrs',
                                                                     np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=10,'1-10Hrs',
                                                                     np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=24,'10-24Hrs',
                                                                              np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=72,'1-3Days',
                                                                                       np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=144,'3-6Days',
                                                                                                np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=240,'6-10Days',
                                                                                                         np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=504,'10-21Days','>21Days'))))))))

# chk = pd.pivot_table(wf_p_with_maxmin[wf_p_with_maxmin['Hired']==1],index=['App_assess_bgc_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()\
                                                                                                                  # np.where(wf_p_with_maxmin['App_assess_bgc_timetaken']/3600<=504,'11-21Days','>21Days'))))))))))
 
wf_p_with_maxmin['App_assess_timetaken_grp'] = np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=0.25,'15mins',
                                                            np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=0.5,'15-30mins',
                                                            np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=1,'30mins-1Hrs',
                                                                     np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=10,'1-10Hrs',
                                                                     np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=24,'10-24Hrs',
                                                                              np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=72,'1-3Days',
                                                                                       np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=168,'3-7Days',
                                                                                                np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=240,'7-10Days',
                                                                                                         np.where(wf_p_with_maxmin['App_assess_timetaken']/3600<=504,'10-21Days','>21Days')))))))))



wf_p_with_maxmin['App_timetaken_grp'] = np.where(wf_p_with_maxmin['Application_timetaken']<=30,'<=30 sec',
                                                            # np.where(wf_p_with_maxmin['Application_timetaken']<=30,'0-30secs',
                                                            np.where(wf_p_with_maxmin['Application_timetaken']<=60,'30-60secs',
                                                                     np.where(wf_p_with_maxmin['Application_timetaken']<=120,'1-2mins',
                                                                     np.where(wf_p_with_maxmin['Application_timetaken']<=180,'2-3mins',
                                                                              np.where(wf_p_with_maxmin['Application_timetaken']<=600,'3-10mins',
                                                                                       # np.where(wf_p_with_maxmin['Application_timetaken']<=420,'5-7mins',
                                                                                                # np.where(wf_p_with_maxmin['Application_timetaken']<=1200,'10-15Mins',
                                                                                                          # np.where(wf_p_with_maxmin['Application_timetaken']<=1200,'10-20Mins',
                                                                                                                   np.where(wf_p_with_maxmin['Application_timetaken']<=3600,'10-60Mins',
                                                                                                                            np.where(wf_p_with_maxmin['Application_timetaken']<=7200,'1-2Hrs',
                                                                                                                                     np.where(wf_p_with_maxmin['Application_timetaken']<=36000,'2-10Hrs','>10hrs'))))))))



######################################## Plots
# App_assess_bgc_time = rankorder(wf_p_with_maxmin,'App_assess_bgc_timetaken',100)
# plot(App_assess_bgc_time['%Hired'])

# App_assess_time = rankorder(wf_p_with_maxmin,'App_assess_timetaken',20)
# plot(App_assess_time['%Hired'])


# App_time = rankorder(wf_p_with_maxmin,'Application_timetaken',100)
# plot(App_time['%Hired'])
######################################
####### Post BGC ##################

wf_p_with_maxmin['Post_BGC_flag'] = np.where((wf_p_with_maxmin['Event'].isin(['BG/Employ Check - Acceptable','Initiate Express Hire']) & (wf_p_with_maxmin['RFstatus']=='Background Check')),1,0)

# wf_p_with_maxmin_subset =wf_p_with_maxmin[wf_p_with_maxmin['Post_BGC_flag']==1]
wf_p_with_maxmin['Post_BGC_flag']  = np.where(wf_p_with_maxmin['Application ID'].isin(list(wf_p_with_maxmin[wf_p_with_maxmin['Post_BGC_flag']==1]['Application ID'])),1,0)

wf_p_with_maxmin['Post_BGC_min_date_final'] = wf_p_with_maxmin['max_bgc_date']

# plot(App_assess_bgc_post_bgc_time['%Hired'])
Application_time = rankorder(wf_p_with_maxmin,'Application_timetaken',5)
Assessment_time = rankorder(wf_p_with_maxmin,'Assessment_timetaken',5)
BGC_time = rankorder(wf_p_with_maxmin,'Background Check_timetaken',5)
Job_Offer_time = rankorder(wf_p_with_maxmin,'Provisional_Job_Offer_timetaken',5)


App_assess_time = rankorder(wf_p_with_maxmin,'App_assess_timetaken',5)
App_assess_bgc_time = rankorder(wf_p_with_maxmin,'App_assess_bgc_timetaken',5)
App_assess_job_bgc_time = rankorder(wf_p_with_maxmin,'App_assess_job_bgc_timetaken',5) 



wf_p_with_maxmin_1 = wf_p_with_maxmin.copy()
# wf_p_with_maxmin = wf_p_with_maxmin_1.copy()

wf_p_with_maxmin = wf_p_with_maxmin_1[wf_p_with_maxmin_1['Application ID'].isin(workflow_unq_jan_mar['application_id'].unique())]
## App time taken Grp
hired = pd.pivot_table(wf_p_with_maxmin[wf_p_with_maxmin['Hired']==1],index=['App_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
application = pd.pivot_table(wf_p_with_maxmin,index=['App_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
App_timetaken_grp = application.rename(columns={'Application ID':'Total_Application'}).merge(hired.rename(columns={'Application ID':'Hired'}),on='App_timetaken_grp',how='left')


# App_assess_timetaken_grp
hired = pd.pivot_table(wf_p_with_maxmin[wf_p_with_maxmin['Hired']==1],index=['App_assess_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
application = pd.pivot_table(wf_p_with_maxmin,index=['App_assess_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
App_assess_timetaken_grp = application.rename(columns={'Application ID':'Total_Application'}).merge(hired.rename(columns={'Application ID':'Hired'}),on='App_assess_timetaken_grp')

######App_assess_bgc_timetaken_grp
hired = pd.pivot_table(wf_p_with_maxmin[wf_p_with_maxmin['Hired']==1],index=['App_assess_bgc_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
application = pd.pivot_table(wf_p_with_maxmin,index=['App_assess_bgc_timetaken_grp'],values='Application ID',aggfunc =pd.Series.nunique).reset_index()
App_assess_bgc_timetaken_grp = application.rename(columns={'Application ID':'Total_Application'}).merge(hired.rename(columns={'Application ID':'Hired'}),on='App_assess_bgc_timetaken_grp')
   

