from distutils.core import setup

setup(
    name='trol',
    packages=['trol'],  # this must be the same as the name above
    version='0.2.1',
    description='A light and predictable Redis object mapper',
    author='Victor "Nate" Graf',
    author_email='nategraf1@gmail.com',
    license='MIT',
    url='https://github.com/nategraf/trol',  # use the URL to the github repo
    download_url='https://github.com/nategraf/trol/archive/0.2.1.tar.gz',
    keywords=['redis', 'object', 'mapper', 'thin',
              'rom', 'redisco'],  # arbitrary keywords
    install_requires=['redis'],
    python_requires='>=3',
    classifiers=[
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        'Operating System :: OS Independent',
        'Natural Language :: English',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',

        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
