--"INTEL CONFIDENTIAL"
--Copyright 2016 Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

-- based on tpc-ds q39
-- This query contains multiple, related iterations:
-- Iteration 1: Calculate the coefficient of variation and mean of every item
-- and warehouse of the given and the consecutive month
-- Iteration 2: Find items that had a coefficient of variation of 1.3 or larger
-- in the given and the consecutive month


DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} AS
SELECT
  inv_warehouse_sk,
  inv_item_sk,
  d_moy,
  cast( ( stdev / mean ) as decimal(15,5)) cov
FROM (
  SELECT
    inv_warehouse_sk,
    inv_item_sk,
    d_moy,    
    cast( stddev_samp( inv_quantity_on_hand ) as decimal(15,5)) stdev,
    cast(         avg( inv_quantity_on_hand ) as decimal(15,5)) mean
  FROM inventory inv
  JOIN date_dim d 
       ON (inv.inv_date_sk = d.d_date_sk
       AND d.d_year = ${hiveconf:q23_year} 
       AND d_moy between ${hiveconf:q23_month} AND (${hiveconf:q23_month} + 1) 
       )
  GROUP BY
    inv_warehouse_sk,
    inv_item_sk,
    d_moy
) q23_tmp_inv_part
-- JOIN warehouse w ON inv_warehouse_sk = w.w_warehouse_sk
WHERE mean > 0 --avoid "div by 0"
  AND stdev/mean >= ${hiveconf:q23_coefficient}
;


--- RESULT --------------------------------------
--keep result human readable
--set hive.exec.compress.output=false;
--set hive.exec.compress.output;

-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${hiveconf:bigbench.hive.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.hive.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.hive.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  inv_warehouse_sk     BIGINT,
  inv_item_sk          BIGINT,
  d_moy                int,
  cov                  decimal(15,5),
  d_moy_consecutive    int,
  cov_consecutive      decimal(15,5)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- Begin: the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
-- Iteration 2: Find items that had a coefficient of variation of 1.5 or larger
-- in the given and the consecutive month
SELECT 
  inv1.inv_warehouse_sk,
  inv1.inv_item_sk,
  inv1.d_moy,
  inv1.cov,
  inv2.d_moy,
  inv2.cov
FROM ${hiveconf:TEMP_TABLE} inv1
JOIN ${hiveconf:TEMP_TABLE} inv2 
    ON(   inv1.inv_warehouse_sk=inv2.inv_warehouse_sk
      AND inv1.inv_item_sk =  inv2.inv_item_sk
      AND inv1.d_moy = ${hiveconf:q23_month}
      AND inv2.d_moy = ${hiveconf:q23_month} + 1
    )
ORDER BY
 inv1.inv_warehouse_sk,
 inv1.inv_item_sk
;
  
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};

  
  