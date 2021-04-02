#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_namespace_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = [ 'aiomisc', 'six', 'avro', 'orjson', 'lmdb', 'pyyaml' ]

setup_requirements = ['pytest-runner',]

test_requirements = ['pytest>=3', ]

setup(
    author="Andrea Mucci",
    author_email='andrea@clariteia.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python Package with common Classes and Utilities used in Minos Microservices",
    install_requires=requirements,
    long_description_content_type='text/markdown',
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='minos_microservice_common',
    name='minos_microservice_common',
    packages=find_namespace_packages(include=['minos.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    version='0.0.1.5-alpha',
    zip_safe=False,
)
