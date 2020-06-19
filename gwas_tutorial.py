# Initial Hail Test
# GWAS Tutorial
!pip3 install hail 

import os
os.environ['HAIL_HOME'] = '/home/cdsw/.local/lib/python3.6/site-packages/hail/backend'
from pyspark.sql import SparkSession
import hail as hl
from hail.plot import show
from pprint import pprint
import time

#################

## FUNCTIONS ##
#################
# Prints the difference in time from start to end
# Format: hh:mm:ss
def timer(message, start):
  hours, remainder = divmod(time.time() - start, 3600)
  minutes, seconds = divmod(remainder, 60)
  print(message, " (hh:mm:ss): ", "{:0>2}:{:0>2}:{:05.2f}"\
        .format(int(hours), int(minutes), seconds))

  return time.time()

################

## MAIN ##
################
## Setup Times
applicationStartTime = time.time()
startTimer = time.time()

## Initialize Hail/Spark/etc
hl.init(tmp_dir='/tmp')

startTimer = timer("Initialize Hail", startTimer)

################################################################################

hl.plot.output_notebook()

################################################################################

hl.utils.get_1kg('/tmp/data/hail/')

################################################################################

hl.import_vcf('/tmp/data/hail/1kg.vcf.bgz').write('/tmp/data/hail/1kg.mt', overwrite=True)

################################################################################

mt = hl.read_matrix_table('/tmp/data/hail/1kg.mt')

################################################################################

mt.rows().select().show(5)
mt.row_key.show(5)
mt.s.show(5)
mt.entry.take(5)

table = (hl.import_table('/tmp/data/hail/1kg_annotations.txt', impute=True)
         .key_by('Sample'))
table.describe()
table.show(width=100)

print(mt.col.dtype)

mt = mt.annotate_cols(pheno = table[mt.s])
mt.col.describe()