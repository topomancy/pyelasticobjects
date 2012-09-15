import os, re, codecs
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def read(*parts):
    return codecs.open(os.path.join(os.path.dirname(__file__), *parts)).read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name="pyelasticobjects",
    version=find_version("pyelasticobjects.py"),
    description="Extension to pyelasticsearch that returns Pythonic objects.",
    #long_description=read('README.rst'),
    author='Schuyler Erle',
    author_email='schuyler@topomancy.com',
    py_modules=['pyelasticobjects'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search'
    ],
    requires=[
        'pyelasticsearch(>=0.1.0)',
    ],
    test_suite='tests',
    url='http://github.com/schuyler/pyelasticobjects'
)
