#"INTEL CONFIDENTIAL"
#Copyright 2016 Intel Corporation All Rights Reserved. 
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

import sys
import logging
import traceback
import os
import time
from time import strftime

timeout = long(sys.argv[1])

if __name__ == "__main__":

	# requires data to be in format <user_sk>\t<timestamp>\t<value>, clustered by user_sk and sorted by <timestamp> ascending
	line = ''
	current_uid = ''
	last_click_time = ''
	perUser_sessionID_counter = 1
	
	try:
		# algorithm expects input lines to be clustered by user_sk and sorted by <timestamp> ascending
		for line in sys.stdin:
		
			user_sk,  tstamp_str, value  = line.strip().split("\t")
			tstamp = long(tstamp_str)

			# reset if next partition beginns
			if current_uid != user_sk:
				current_uid = user_sk
				perUser_sessionID_counter = 1
				last_click_time=tstamp

			# time between clicks exceeds session timeout?
			if tstamp - last_click_time > timeout:
				perUser_sessionID_counter += 1
				
			last_click_time =tstamp
			print "%s\t%s\t%s_%s" % (value, tstamp_str, user_sk, str(perUser_sessionID_counter) )

	except:
		## should only happen if input format is not correct, like 4 instead of 5 tab separated values
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/bigbench_q4-sessionzie.py_%Y%m%d-%H%M%S.log"))
		logging.info("sys.argv[1] timeout: " +str(timeout) + " line from hive: \"" + line + "\"")
		logging.exception("Oops:") 
		raise
		sys.exit(1)
