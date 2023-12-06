import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='SOURCE',
    version='1.0.0',
    author='Paolo Oliveri',
    data_files=['LICENSE.txt'],
    author_email='paolo.oliveri@ingv.it',
    description='A Sea Observations Utility for Reprocessing, Calibration and Evaluation',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/fair-ease/Source',
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Creative Commons :: CC BY SA NC",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

# package_data={'SOURCE': ['obs_postpro/*.csv']},


# include SOURCE/obs_postpro/probes_names.csv,
# include SOURCE/obs_postpro/url_countries.csv,
#    packages=setuptools.find_packages(where='SOURCE'),
#    package_dir={'SOURCE': 'SOURCE'},
