from setuptools import setup


setup(name='filesdb',
      version='0.1.0',
      description='A simple tool for tracking files in a database',
      author='Steven Tilley',
      author_email='steventilleyii@gmail.com',
      packages=['filesdb'],
      entry_points={'console_scripts': ['filesdb = filesdb.filesdb:main']},
      )
