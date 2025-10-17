# How to contribute

First off, thank you for taking the time to contribute! If you have a function that you would like to see in the National Reusable Code Library, we have a few standards and guidelines that we would like you to follow before we consider your merge request. Failure to follow the contribution guide will result in your merge request being challenged or rejected.

We are looking for reusable functions and/or classes which are useful across multiple workflows in NHS data processing and analytics. Please do not submit the following...
* End-to-end scripts that cannot be implemented into the package. If you find something reusable within your end-to-end script, then please feel free to extract it and submit it with its unit tests attached.
* Duplicated functionality. For example, if your function is already done by another well known package.
* Irrelevant functionality. If the function you submit is unrelated to DIS, it will mostly likely be challenged or rejected.

When making a submission, it is helpful if this is kept small, and that the content is all related / follows a theme / are different methods of a class. It will make review and acceptance more straightforward.

## Eligibilty
Anyone is free to suggest contributions this repository, access it, and utilise its contents. However, as a number of steps for the approval process take place within NHS England, if you're external it might be wise to email the repository owners to discuss your submission so that they can bring it through the governance and approval process within NHS England. Otherwise you might end up waiting until someone notices a new pull request and considers it.

The process for submitting new code is:

1. Identify the reusable components within your code and refactor them to be standalone functions / classes (e.g. no integrated or dependent on the work they were originally a part of)
2. [Fork](https://help.github.com/en/articles/fork-a-repo) this repo on GitHub.
4. Write your documented function and tests (:heart_eyes:) on a new branch, coding in line with our **coding standards**.
5. Submit a [pull request](https://help.github.com/en/articles/creating-a-pull-request) **main branch** of the National Reusable Code Library with a clear description of what you have done, fully completing all sections of the  `pull_request_template.md` (the empty template should appear automatically in the pull request).
6. The maintainers of the repository will review the code, and, if it meets the basic standards set out here, take if for consideration to the Reusable Code Advisory Group within NHS England.
7. If rejected, feedback will be provided, and once this has been actioned it can taken back to the Advisory Group
8. IF accepted, the pull request will be merged into the main branch.
9. At regular interverals, the code will be packaged up and released - the commit will be tagged with a semantic version, following the usual approach of trivial, minor and major (breaking) changes. These versioned packages will be published to PyPi and potentially other package repositories.

We suggest you make sure all of your commits are atomic (one feature per commit). Please make sure that non-obvious lines of code are commented, and variable names are as clear as possible. Please do not send us undocumented code as we will not accept it. Including tests to your pull request will bring tears of joy to our eyes, and will also probably result in a faster merge.

## Coding standards

In short:
- Follow PEP-8, and use Black to enforce this on your code
- Have unit tests for all your functions / methods
- Write [Numpy-style](https://numpydoc.readthedocs.io/en/latest/format.html#docstring-standard) docstrings for functions, including descriptions, inputs, outputs, and example usage
- Methods used in the code should have been approved by the appropriate body, e.g. the Data Quality Steering Group for DQ rules. For external submissions, this might require a longer review process in which these are brought to those bodies.

We use the industry standard [PEP 8](https://www.python.org/dev/peps/pep-0008/) styling guide. **Therefore, it’s imperative that you use the coding standards found within PEP 8 when creating or modifying any code within the repository**. Autoformatters for PEP8, for instance [black](https://black.readthedocs.io/en/stable/), can easily ensure compliance. The reason we use PEP 8 coding standards is to make sure there is a layer of consistency across our codebase. This reduces the number of decisions that you need to make when styling your code, and also makes code easier to read when switching between functions etc.

The following articles can be useful when writing code to the standard:
* [Code layout](https://www.python.org/dev/peps/pep-0008/#code-lay-out) – Indentation, tabs or spaces, maximum line length, blank lines, source file encoding, imports & module level Dunder name
* [String quotes](https://www.python.org/dev/peps/pep-0008/#string-quotes)
* [Whitespace in expressions and statements](https://www.python.org/dev/peps/pep-0008/#whitespace-in-expressions-and-statements) – Pet Peeves, alternative recommendations
* [When to use trailing commas](https://www.python.org/dev/peps/pep-0008/#when-to-use-trailing-commas)
* [Comments](https://www.python.org/dev/peps/pep-0008/#comments) – Block comments, inline comments & documentation strings (docstrings)
* [Naming conventions](https://www.python.org/dev/peps/pep-0008/#naming-conventions) – Naming styles, naming conventions, names to avoid, ASCII compatibility, package and module names, class names, type variable names, exception names, global variable names, function and variable names, function and method arguments, method names and instance variables, constants & designing for inheritance
* [Programming recommendations](https://www.python.org/dev/peps/pep-0008/#programming-recommendations) – Function annotations & variable annotations


## Community

The discussions space on this github repo is the perfect place to discuss the code and reach out to those working on it.

## Code of Conduct

As a contributer you can help us keep the this community open and inclusive. Please read and follow our [Code of Conduct](https://github.com/codonlibrary/code-of-conduct/tree/master). By contributing to it, you agree to comply with it.
