# prod-fixer-python-script
Small python script that works as a guide to fix issues in production with shipments, packs, etc.

# Python version

This repository is made in Python 3.6.13 and pip3 (version 22.1.2). Make sure you have those installed in your system.

# The small fix made with the run.py script

The problem is basically to remove the oldest of the *split* or *cancel* tag.

# Using the script
The idea of having this repo is basically take either the **run.py** script or the **script_guide.py** script as a starting point to programmatically solve new problems with data of shipments or packs in production.

There are severals useful tools and tricks in the **script_guide.py**, several were used to design and implement the **run.py** script. Mainly the **script_guide**, that solve other problem, should be the main reference for future issues to fix.

## Output format

The programs standard output will have two columns, in the first the shipments ids and in the second a message with the result of the process.

```
41449531583	OK
41467087812	OK
41463861427	No double tags
41449544994	No double tags
41469065011	No double tags
41386106580	No double tags
41386101898	sibling_source not split nor partial_cancellation
```

## Use virtual environment
* A good idea for this project, is to use a virtual environment, you could set up one with: [virtualenv](https://virtualenv.pypa.io/en/latest/).
* To create the virtual environment: `virtualenv env`
* To activate it:`source env/bin/activate`
* To install dependencies: `pip3 install -r requirements.txt`
* To deactivate the virtual environment run `deactivate`

## Running the program

After being in the virtual environment and installing all dependencies, to run de program simply run `python3 run.py`. This should look something like:

`(venv) ➜  prod-fixer-python-script git:(main) python3 run.py`

To get the output in a text file, run:  `python3 run.py > output_example.txt` to get the output in the file `output_example.txt`.

## Black Prettier

This code has been beautify using black: https://github.com/psf/black. 
The command to use is `black . -l 120`.

## Contribution

All MeLi contribution are welcome. Make sure to test everything and run `black . -l 120` before making any pull request. 

# run_integrator_detection.py

Script to combine output form the audit tool on fury with what the shipments get endpoints return. Easily customizable. It uses input from file passed as parameter, print some info messages in its executions, but the generated cvs is the second argument for example:

```python3 run_integrator_detection.py input_file.csv desirable_output_file.csv```

For example, with a given small input file: 

```python3 run_integrator_detection.py small-audit-splitter-400.csv small-output.csv```

The script also produces a file META_DATAdesirable_output_file.csv in which are some counting of interest.

# Rnn run_fulfillment_errors_printer

This script uses a hard-coded token and a hard-coded, it takes as argument an input file in a specific format and the name of the output file to print: 

Example

```python3 run_fulfillment_errors_printer.py 2022-11-24.csv 2022-11-24output.txt```