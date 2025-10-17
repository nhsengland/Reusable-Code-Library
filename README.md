# National Reusable Code Library 🌍
[![CI](https://github.com/nhsengland/reusable-code-library/actions/workflows/ci.yml/badge.svg)](https://github.com/nhsengland/reusable-code-library/actions/workflows/ci.yml) 
![Static Badge](https://img.shields.io/badge/status-development-blue) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Code Standard: RAP](https://img.shields.io/badge/code%20standard-RAP-000099.svg)](https://nhsdigital.github.io/rap-community-of-practice/) 
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is the `National Reusable Code Library`?

The `National Reusable Code Library` is intended to agree, centralise and enable to reuse of common methods within the Health and Care sector. This will encourage consistancy of analysis, code and coding standards and encourage collaboration. The repository contains Python, R and SQL code for ease of use in workflows on platforms used within the NHS. Package documentation will be available shortly, but each function and module should have a docstring. 

The `National Reusable Code Library` aims to reduce the barrier for entry for analysis and data engineering, meaning colleagues need to develop less new code, and instead giving them the opportunity to help improve and reuse existing code, also making the opportunity of more advanced software development experience for those at a higher level of technical ability. 

### Why do we need the `National Reusable Code Library`?

Briefly, our colleagues write a lot of the same stuff, again and again. This by itself is wasteful, but it also leads to inconsistency, error and a lack of improvement, not to mention it's boring and doesn't stretch our colleagues and allow them to learn and build.

By making code reusable, and making it easy to reuse, this work aims to:

**Increase Transparency**: To align with government data principles and build public trust.

**Improve Code**: To innovate and improve the code we use and provide.

**Improve usability**: By increasing the accessibility and uniformity of code, it becomes easier for data users to find and use relevant code.

**Be more cost effective**: Reusable 'generalised' code will increase efficiency in creating higher level processes.

## Installation 
The package can be directly installed as a python package from PyPi by in your terminal: 
```terminal
pip install nhs_reusable_code_library
```
Other platform specific instructions to follow.

## How to use the National Reusabel Code Library package
When using Python, given that you've installed the package as described above, you can simply [import](https://docs.python.org/3/tutorial/modules.html#packages) it as normal:

```Python
from nhs_reusable_code_library.standard_data_validations.nhsNumberValidation import mod11_check

nhs_number = '1111111111'

nhs_number_valid = mod11_check(nhs_number)
```

## Code Manfiest
(Made using "[project-tree-generator](https://project-tree-generator.netlify.app/generate-tree)")

```
Reusable-Code-Library/
├── .github/ # Directory for github specific templates and CI/CD (github actions)
│   ├── ISSUE_TEMPLATE/ # templates for when people raise issues
│   ├── pull_request_template.md # template used when a pull request is raised
│   └── workflows/ # Github Actions (CI/CD) pipelines go here
│       └── ci.yml # This is the Continuous Integration pipeline which runs the unit tests and tests the package builds
└── src/
│   └── nhs_reusable_code_library/ # the main package directory which will have a number of libraries
│       ├── standard_data_validations/ # the place for data quality rules code
│       │   ├── nhsNumberValidation/ # NHS number validation related code
│       │   ├── polars/ # Polars implementations of data quality rules code
│       │   └── pyspark/ # PySpark implementations of data quality ruls code
│       └── tests/ # the unit tests for the functions within the package
├── .gitignore # tells the repo which files to ignore, e.g. temporary, hidden and background files, and outputs.
├── CONTRIBUTING.md # Describes how to contribute to the repository
├── LICENSE # Describes the License the code can be used under.
├── pyproject.toml # Used when building the package
└── README.md # Describes what the package is for and how to use it.
```

## Governance
New reusable code is discussed and signed off in the Reusable Code Assurance Group within NHS England. This group also sets the standards this code is made to.
New code must have appropriate unit tests and all unit tests must pass before it can be merged into the main branch. These tests can be found in the `src/.../tests` folders.

## Contributing
All new contributions to the `National Reusable Code Library` are welcome; please follow the guidance document for contributions. 

Any improvements to documentation, bug fixes or general code enhancements are also welcomed. If a bug is found on the master branch, please use the GitHub guidance on raising an [issue.](https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue)

## New to GitHub?
GitHub is a hosting site that allows for development and version control of software using Git. It allows users to edit and develop parts of code independently before submitting back to the master code, whilst using version control to track changes. Introductory guidance can be found here: [https://nhsdigital.github.io/rap-community-of-practice/training_resources/git/introduction-to-git/]

## Acknowledgments
Thanks in particular to the amazing work of both the [NHS Digital RAP Squad](https://nhsdigital.github.io/rap-community-of-practice), and the [NHS Codon Project](https://github.com/codonlibrary/codonPython) who greatly inspired this work and set the foundations for it years ago.

## Contact
NHS England Data Architecture Team
