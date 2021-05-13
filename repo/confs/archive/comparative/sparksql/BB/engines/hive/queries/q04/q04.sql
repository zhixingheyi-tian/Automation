--"INTEL CONFIDENTIAL"
--Copyright 2016 Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.


-- TASK
-- Web_clickstream shopping cart abandonment analysis: For users who added products in
-- their shopping carts but did not check out in the online store during their session, find the average
-- number of pages they visited during their sessions. 
-- A "session" relates to a click_session of a known user with a session time-out of 60min.
-- If the duration between two clicks of a user is greater then the session time-out, a new session begins.

--IMPLEMENTATION NOTICE
-- The difficulty is to reconstruct a users browsing session from the web_clickstreams  table
-- There are are several ways to to "sessionize", common to all is the creation of a unique virtual time stamp from the date and time serial
-- key's as we know they are both strictly monotonic increasing in order of time and one wcs_click_date_sk relates to excatly one day
--  the following code works: (wcs_click_date_sk*24*60*60 + wcs_click_time_sk
-- Implemented is way B) as A) proved to be inefficient
-- A) sessionize using SQL-windowing functions => partition by user and  sort by virtual time stamp.
--    Step1: compute time difference to preceding click_session
--    Step2: compute session id per user by defining a session as: clicks no father apart then q02_session_timeout_inSec
--    Step3: make unique session identifier <user_sk>_<user_session_ID>
-- B) sessionize by clustering on user_sk and sorting by virtual time stamp then feeding the output through a external reducer script
--    which linearly iterates over the rows,  keeps track of session id's per user based on the specified session timeout and makes the unique session identifier <user_sk>_<user_seesion_ID>

-- Resources
ADD FILE ${hiveconf:QUERY_DIR}/q4_abandonedShoppingCarts.py;
ADD FILE ${hiveconf:QUERY_DIR}/q4_sessionize.py;
--set hive.exec.parallel = true;


-- Part 1: re-construct a click session of a user -----------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT *
FROM
(
  FROM
  (
    SELECT
      c.wcs_user_sk,
      w.wp_type,
      (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp_inSec
    FROM web_clickstreams c, web_page w
    WHERE c.wcs_web_page_sk = w.wp_web_page_sk
    AND   c.wcs_web_page_sk IS NOT NULL
    AND   c.wcs_user_sk     IS NOT NULL
    AND   c.wcs_sales_sk    IS NULL
    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec
  ) clicksAnWebPageType
  REDUCE
    wcs_user_sk,
    tstamp_inSec,
    wp_type
  USING 'python q4_sessionize.py ${hiveconf:q04_session_timeout_inSec}'
  AS (
    wp_type STRING,
    tstamp BIGINT,
    sessionid STRING)
) sessionized
;


--Result  --------------------------------------------------------------------
--keep result human readable
--set hive.exec.compress.output=false;
--set hive.exec.compress.output;
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  averageNumberOfPages DECIMAL(20,1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';


INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT SUM(pagecount) / COUNT(*)
FROM
(
  FROM
  (
    SELECT *
    FROM ${hiveconf:TEMP_TABLE1} sessions
    DISTRIBUTE BY sessionid SORT BY sessionid, tstamp, wp_type --required by "abandonment analysis script"
  ) distributedSessions
  REDUCE 
    wp_type,
    sessionid
    USING 'python q4_abandonedShoppingCarts.py'
    AS (pagecount BIGINT)
) abandonedShoppingCartsPageCountsPerSession
;

--cleanup --------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
