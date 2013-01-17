from setuptools import setup

setup(
    name = 'lxid',
    version = '0.1',
    description = 'Likelihood-based background identification',
    author = 'Andy Mastbaum',
    author_email = 'mastbaum@hep.upenn.com',
    url = 'http://github.com/mastbaum/lxid',
    packages = ['lxid'],
    scripts = ['bin/convert_events.py'],
    install_requires = ['pyzmq-static']
)

