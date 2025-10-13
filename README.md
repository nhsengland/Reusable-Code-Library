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
├── .github/ # Directory for 
│   ├── ISSUE_TEMPLATE/
│   ├── pull_request_template.md
│   └── workflows/
│       └── ci.yml
└── src/
│   └── nhs_reusable_code_library/
│       ├── standard_data_validations/
│       │   ├── nhsNumberValidation/
│       │   ├── polars/
│       │   └── pyspark/
│       └── tests/
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── pyproject.toml
└── README.md
```

## Governance

## Contributing
All new contributions to the `National Reusable Code Library` are welcome; please follow the Coding Conventions in the guidance document for contribution guidance. 

Any improvements to documentation, bug fixes or general code enhancements are also welcomed. If a bug is found on the master branch, please use the GitHub guidance on raising an [issue.](https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue)

## New to GitHub?
GitHub is a hosting site that allows for development and version control of software using Git. It allows users to edit and develop parts of code independently before submitting back to the master code, whilst using version control to track changes. Introductory videos to GitHub for beginners can be found [here.](https://github.com/codonlibrary/codonPython/wiki/2a.-GitHub-for-Beginners) 

Quick links to beginner guidance can also be found below:

* [**Cloning a repository to your local machine using GitBash**](https://github.com/codonlibrary/codonPython/wiki/1.-Installing-codonPython)
* [**Checking out a branch using GitBash**](https://github.com/codonlibrary/codonPython/wiki/2b.-Checkout-a-branch-using-GitBash)
* [**Removing a Commit from a repository using GitBash**](https://github.com/codonlibrary/codonPython/wiki/3.-Removing-a-Commit-From-a-GitHub-Repository)

All other `codon` "How-to Articles" can be found [here.](https://github.com/codonlibrary/codonPython/wiki/2.-Git-Guidance)

Suggestions regarding additional guidance or How-to articles are welcome.

## Acknowledgments

## Contact
