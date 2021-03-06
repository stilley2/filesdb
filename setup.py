from setuptools import setup


setup(name='filesdb',
      version='2.2.2',
      description='A simple tool for tracking files',
      author='Steven Tilley',
      author_email='steventilleyii@gmail.com',
      scripts=['test_filesdb.py'],
      packages=['filesdb'],
      entry_points={'console_scripts': ['filesdb = filesdb.__main__:main']},
      python_requires='>=3',
      tests_require=['pytest'],
      setup_requires=['pytest-runner'],
      url='https://github.com/stilley2/filesdb',
      license='MIT',
      )
