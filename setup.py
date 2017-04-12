from setuptools import setup

setup(name='optimus',
      version='0.0.1',
      description='Optimus transforms and rolls out your csv data to redcap.',
      url='http://github.com/pfwhite/optimus',
      author='Patrick White',
      author_email='pfwhite9@gmail.com',
      license='MIT',
      packages=['optimus'],
      entry_points={
          'console_scripts': [
              'optimus = optimus.__main__:cli_run',
          ],
      },
      install_requires=['docopt', 'pyyaml', 'python-dateutil'],
      zip_safe=False)
