"""
Setuptools setup file.
"""
from setuptools import setup, find_packages

with open('requirements.txt', 'r') as file:
    requirements = [x.strip() for x in file.readlines() if x and x.strip()]

print(f'Dependencies are: {requirements}')

setup(
    name='slr_helper',
    version='0.1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=requirements,
    license='MIT',
    author='makrei-p',
    url='https://github.com/makrei-p/slr_helper'
)
