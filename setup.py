from setuptools import find_packages, setup

setup(
    name="chicago_businesses_pipeline",
    version="0.1",
    author="Michael Gallagher",
    author_email="mjgall@pm.me",
    license="MIT",
    url="https://github.com/michaeljgallagher",
    packages=find_packages(),
    install_requires=open("requirements.txt").readlines(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    package_data={
        "": ["pipeline.ini"],
    },
    entry_points={
        "console_scripts": ["chi-data-pipeline=chicago_businesses_pipeline.main:main"],
    },
)
