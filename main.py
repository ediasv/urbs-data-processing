# this file should be run from the root of the repository
# binds all of the scripts
# 1. download
# 2. uncompress
# 3. trust
# 4. refined ingestion (all)
# 5. upload to huggingface

# accepts yyyy-mm argument and runs for that month
# runs all scripts sequentially for that month
# use calendar to get the number of days in the month
# should "replace" the gpu-demo.sh script (encapsulates all of the python scripts)

# maybe it will be necessary to modify the other scripts to be called from this script
# instead of running them individually through the terminal
