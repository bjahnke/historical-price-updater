#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [
    "data_manager @ git+https://github.com/bjahnke/data_manager.git#egg=data_manager",
    "sqlalchemy",
    "pandas",
]

test_requirements = [
    "pytest>=3",
]

setup(
    author="Brian Jahnke",
    author_email="bjahnke71@gmail.com",
    python_requires=">=3.6",
    description="download stock data, reformat, push to database",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="historical_price_updator",
    name="historical_price_updator",
    packages=find_packages(
        include=["historical_price_updator", "historical_price_updator.*"]
    ),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/bjahnke71/historical_price_updater",
    version="0.1.1",
    zip_safe=False,
)
