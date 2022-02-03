import os
import sys
os.environ['pipo'] = '/shared/.pipo'
from pipo.common.interfaces import logger 

logr = logger()

def end_write_with_fail(stage):
	logr.task_result_code= '-1'
	logr.task_result_desc = "ERROR:: while writing to dwh stage "+stage
	logr.task_state = "finished" 
	########### to add more functions ###########
	########### to add more functions ###########
	########### to add more functions ###########
	########### to add more functions ###########
	########### to add more functions ###########
	
	
	
def end_write_with_success(stage)
	logr.task_result_code= '200'
	logr.task_result_desc = "INFO:: "+stage +" is DONE successfully"
	logr.task_state = "finished" 
	########### to add more functions ########### 	
	########### to add more functions ###########
	########### to add more functions ###########
	########### to add more functions ###########
	########### to add more functions ###########