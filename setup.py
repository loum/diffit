"""Setup script.
"""
import os
import setuptools


class Packaging(setuptools.Command):
    """Common PyPI packaging tools.

    """
    user_options = []
    def initialize_options(self):
        """Dummy override.
        """

    def finalize_options(self):
        """Dummy override.
        """

    def run(self): #pylint: disable=no-self-use
        """Clean up temporary package build files.

        """
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')

PROD_PACKAGES = [
    'filester',
    'pyspark',
]

DEV_PACKAGES = [
    'pipdeptree',
    'pylint',
    'pytest',
    'pytest-cov',
    'pytest-sugar',
    'twine',
]

PACKAGES = list(PROD_PACKAGES)
if (os.environ.get('APP_ENV') and 'local' in os.environ.get('APP_ENV')):
    PACKAGES.extend(DEV_PACKAGES)

SETUP_KWARGS = {
    'name': 'diffit',
    'version': os.environ.get('MAKESTER__RELEASE_VERSION', '1.0.0'),
    'description': 'Data Differ',
    'author': 'Lou Markovski',
    'author_email': 'lou.markovski@gmail.com',
    'url': 'https://github.com/loum/differ',
    'install_requires': PACKAGES,
    'package_dir': {'': 'src'},
    'packages': setuptools.find_namespace_packages(where='src'),
    'package_data': {'diffit.schema': ['data/*.json']},
    'scripts': ['src/bin/differ'],
    'include_package_data': True,
    'license': 'MIT',
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    'cmdclass': {'clean': Packaging},
}

setuptools.setup(**SETUP_KWARGS)
