[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-f059dc9a6f8d3a56e377f745f24479a46679e63a5d9fe6f495e02850cd0d8118.svg)](https://classroom.github.com/online_ide?assignment_repo_id=6534844&assignment_repo_type=AssignmentRepo)
# Data Acquisition and Processing Systems (DaPS) (ELEC0136)

Welcome to the final assignment of the _Data Acquisition and Processing Systems_ (DaPS) course ELEC0136 at UCL.

The objective of this assignment is to simulate a real-life data-science situation that can be
approached using the process described in class: i) finding a source of data, ii) acquiring and
storing it iii) cleaning and preprocessing it, iv) extracting meaningful visualisations, v) building a
model for inference. You are also free to use any additional methods you find are well suited for
the problem.
The outcome of this assignment consists of two deliverables: a) a written report in the form of an
academic paper and, b) the code-base to support it. You are allowed to discuss ideas with peers,
but your code, experiments and report must be done solely based on your own work.

This repository contains the scaffolding for submitting your code.
Here are some must and some advices on how to write your code.

- Must:
  - Have a [conda environment file](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually), name `environment.yml`, which only specifies the python version you are using. Do not export your environment automatically. __The environment must be named daps.__
  - Have a [pip requirements file](https://pip.pypa.io/en/stable/user_guide/#requirements-files), named`requirements.txt` file in the root of this repository.
  - Have an `assignment.ipynb` notebook file that contains a `def main():` function. This function must run all the code necessary to produce the figures, tables and any other ingredient that you included in the written report. Write your code in a way that this material is saved on disk. Plotting this material on screen will result in errors. You can write all your assignment in a python file, or any other method your prefer, but you must use the notebook to write this function.
  - Do not modify or delete the `.github/workflows/build.yml`. This will be used for automatic checks.
  
- Advices:
  - Make your code readable, don't overthink its performance. This is an academic assignment, not production code.
  - Work on the `main` branch of this repository ( This is between and advice and a must).
  - __Remember to push your code to remote before submitting.__
  - Use the __automatically generated__ `feedback` pull request (__do not__ open another pull request) to comment on your own code, clarify obscure parts, or fancy computations.
  - The notebook is only __an example__ on how to solve this assignment. Other than the `main` function, there is no need to use this template.


The courious ones can check the `.github/workflows/build.yml` file to see what the automatic checks will do.

### Submitting your assignment

To submit your assignment, please comment on the `feedback` PR with `@epignatelli, @Sephora-M, @tonizza, I submit!`. This way we will receive the notification you submitted and know that the assignment is completed.
If you submit after the deadline, every commit with timestamp later than the deadline will not be considered. 


Good luck!
