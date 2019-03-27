# Connect to NYU HPC
### In campus: 
ssh NetId@dumbo.es.its.nyu.edu
### Off campus: 
- ssh NetId@gw.hpc.nyu.edu
- ssh NetId@dumbo.es.its.nyu.edu

# Add file locally 
(in local terminal)

scp file_name NetId@dumbo.es.its.nyu.edu:~/.

# Initialize
module load spark/2.4.0 python/gnu/3.4.4 gcc/5.3.0

export PYSPARK_PYTHON=\`which python`

hadoop fs -put file_name

spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH --files myshapefile.geojson myscript.py outputfolder





