from setuptools import setup

#bring in __version__ from sourcecode
#per https://stackoverflow.com/a/17626524
#and https://stackoverflow.com/a/2073599

with open('optimus/version.py') as ver:
    exec(ver.read())

setup(name='optimus',
      version=__version__,
      description='Optimus transforms and rolls out your csv data to redcap.',
      url='http://github.com/pfwhite/optimus',
      author='Patrick White',
      author_email='pfwhite9@gmail.com',
      license='Apache 2.0',
      packages=['optimus'],
      entry_points={
          'console_scripts': [
              'optimus = optimus.__main__:cli_run',
          ],
      },
      install_requires=[
          'docopt==0.6.2',
          'pyyaml==3.12',
          'python-dateutil==2.6.1'],
      zip_safe=False)
