#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = []

test_requirements = ['pytest>=3', ]

setup(
    author="Paulo Radatz",
    author_email='paulo.radatz@gmail.com',
    python_requires='>=3.10',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
    ],
    description="Short description",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='bdgd2opendss',
    name='bdgd2opendss',
    packages=find_packages(include=['bdgd2opendss', 'bdgd2opendss.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/PauloRadatz/bdgd2opendss',
    version='1.0.0',
    zip_safe=False,
)
