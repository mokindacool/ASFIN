# Introduction
ASUC Finance Transformations (ASFINT) is a library containing rudimentary cleaning and transformation scripts that are used to process raw data into cleaned datasets meant to serve as the source of truth on the OCFO Financial Database's warehouse level. 
Functions are design both to be called manually for manual cleaning of records and integrated into automatic workflows for realtime cleaning and integration into the OCFO Database system. 

ASFINT does NOT contain any scripts beyond the data transformation scripts and is agnostic to backend platforms supporting data push and pull functionalities to external cloud platforms. 

# How to run the workflow
First make a conda enviornment with the necessary dependencies. Move to the root of this repo then type the following into your terminal 
- `conda create -n ASFINT0.1 python=3.11.8`
- `conda activate ASFINT0.1`
- `pip install -r requirements.txt`
- `pip install -e .`

If you have errors with this step check your working directory with `pwd` and make sure the returned path ends with `ASFIN`. Eg: 
`/Users/yourusername/Desktop/ASFIN`

Then make sure you have files to clean. All raw files to be cleaned should be inputted into `ASFIN/files/input` and the resulting cleaned files should be outputted into `ASFIN/files/output`. If these folders do not exist please make them. 

Then set the settings you need in `execute.py`. The only thing you have to change is the `processType` field in the `settings` dictionary for different kinds of datasets. **If you're cleaning the FR dataset use the `FR` process type.**

Then start running the workflow. Make sure you're in the working directory, your terminal should display something like this:
`(ASFINT0.1) yourusername@wifi-XX-XX-XX-XXX ASFIN % `

Then type in: `python execute.py` and you should be good. 

Debug as needed. 

# How does ASFIN work?
The main ETL functions labelled pull, process and push are located in `ASFINT/Pipeline/workflow.py`. Files in the root like `execute.py` (execites programmatically) and `run.py` (executes via terminal) call the entire workflow to run. `ASFINT/Transform/processory.py` contains all the nitty gritty transformation functions and `ASFINT/Config/config.py` contains all the settings for different datasets/process types the workflow expects and currently supports. 
