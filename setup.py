from distutils.core import setup

setup(
  name = 'trol',
  packages = ['trol'], # this must be the same as the name above
  version = '0.1',
  description = 'A light and predictable Redis object mapper',
  author = 'Victor "Nate" Graf',
  author_email = 'nategraf1@gmail.com',
  url = 'https://github.com/nategraf/trol', # use the URL to the github repo
  download_url = 'https://github.com/nategraf/trol/archive/0.1.tar.gz',
  keywords = ['redis', 'object', 'mapper', 'thin', 'rom', 'redisco'], # arbitrary keywords
  classifiers = [],
)
